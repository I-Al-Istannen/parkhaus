use crate::data::{PendingMigration, S3Object, S3ObjectId, TieringRule, UpstreamId};
use crate::policy::expr::{Env, Type};
use crate::policy::tier_rule;
use crate::policy::tier_rule::SqlArgument;
use jiff::{Timestamp, Zoned};
use rootcause::Report;
use rootcause::prelude::ResultExt;
use sqlx::{FromRow, Sqlite, SqliteConnection, query, query_as};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TieringRuleEnv;

impl TieringRuleEnv {
    pub fn synthesize_variable_sql(name: &str, now_var: &str) -> String {
        match name {
            "age" => format!("({now_var} - last_modified)"),
            "last_accessed" => format!("({now_var} - last_accessed)"),
            "bucket" => "bucket".to_string(),
            "object" => "(bucket || '/' || key)".to_string(),
            "key" => "key".to_string(),
            "upstream" => "assigned_upstream".to_string(),
            "size" => "size".to_string(),
            _ => unreachable!("Unknown variable: {}", name),
        }
    }

    pub fn synthesize_function_sql(name: &str, now_var: &str, args: &[String]) -> String {
        match name {
            "access_counts" => {
                // 1d, 10d => swap order
                // We do not need to truncate the now_var to a date, as e.g.
                // 12.05. 15:00 | 1d, 10d => 02.05. 15:00, 11.05. 15:00
                // We then move the start by one day back and get
                // 01.05. 15:00 until 11.05. 15:00
                // 01.05 15:00 ... 02.05. 00:00 ... 11.04. 00:00 ... 11.05. 15:00
                //                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                //                       wanted timerange
                // Which encloses the wanted timerange but no other bucket.
                format!(
                    "(SELECT SUM(count) FROM AccessCounters WHERE \
                        obj_bucket = bucket AND \
                        obj_key = key AND \
                        time_bucket BETWEEN ({now_var} - {} - 86400000) AND ({now_var} - {}))",
                    args[1], args[0]
                )
            }
            _ => unreachable!("Unknown function: {}", name),
        }
    }
}

impl Env for TieringRuleEnv {
    fn get_var(name: &str) -> Option<Type> {
        match name {
            "age" => Some(Type::Number),
            "last_accessed" => Some(Type::Number),
            "bucket" => Some(Type::String),
            "object" => Some(Type::String),
            "key" => Some(Type::String),
            "upstream" => Some(Type::String),
            "size" => Some(Type::Number),
            _ => None,
        }
    }

    fn get_fun(name: &str) -> Option<(Vec<Type>, Type)> {
        match name {
            "access_counts" => Some((vec![Type::Number, Type::Number], Type::Number)),
            _ => None,
        }
    }
}

#[derive(FromRow)]
pub struct DbObject {
    bucket: String,
    key: String,
    assigned_upstream: UpstreamId,
    last_modified: i64,
    size: i64,
}

impl TryFrom<DbObject> for S3Object {
    type Error = Report;

    fn try_from(db_obj: DbObject) -> Result<Self, Report> {
        let id = S3ObjectId {
            bucket: db_obj.bucket,
            key: db_obj.key,
        };
        let last_modified = Timestamp::from_millisecond(db_obj.last_modified)
            .context("invalid last_modified timestamp")
            .attach(format!("object: {id}"))?;

        Ok(Self {
            id,
            assigned_upstream: db_obj.assigned_upstream,
            last_modified,
            size: db_obj.size as u64,
        })
    }
}

pub(super) async fn get_object(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
) -> Result<S3Object, Report> {
    query_as!(
        DbObject,
        r#"
        SELECT
            bucket, key, assigned_upstream, last_modified, size
        FROM objects
        WHERE bucket = $1 AND key = $2"#,
        obj.bucket,
        obj.key
    )
    .fetch_one(con)
    .await
    .context("failed to fetch object")
    .attach(format!("object: {obj}"))?
    .try_into()
}

pub(super) async fn get_upstream(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
) -> Result<Option<UpstreamId>, Report> {
    Ok(query!(
        "SELECT assigned_upstream FROM objects WHERE bucket = $1 AND key = $2",
        obj.bucket,
        obj.key
    )
    .map(|row| row.assigned_upstream)
    .fetch_optional(con)
    .await
    .context("failed to fetch object")
    .attach(format!("object: {obj}"))?
    .map(UpstreamId))
}

pub(super) async fn delete_object(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
) -> Result<(), Report> {
    query!(
        "DELETE FROM objects WHERE bucket = $1 AND key = $2",
        obj.bucket,
        obj.key
    )
    .execute(con)
    .await
    .context("failed to delete object")
    .attach(format!("object: {obj}"))?;

    Ok(())
}

pub(super) async fn record_creation(
    con: &mut SqliteConnection,
    obj: &S3Object,
) -> Result<(), Report> {
    let last_modified = obj.last_modified.as_millisecond();
    let size = obj.size as i64;

    query!(
        r#"
        INSERT INTO objects (bucket, key, assigned_upstream, last_modified, last_accessed, size)
        VALUES ($1, $2, $3, $4, $4, $5)
        ON CONFLICT (bucket, key) DO UPDATE SET
            assigned_upstream = excluded.assigned_upstream,
            size = excluded.size,
            last_modified = excluded.last_modified,
            last_accessed = excluded.last_accessed
        "#,
        obj.id.bucket,
        obj.id.key,
        obj.assigned_upstream,
        last_modified,
        size
    )
    .execute(con)
    .await
    .context("failed to record object creation")
    .attach(format!("object: {}", obj.id))
    .attach(format!("upstream: {}", obj.assigned_upstream))?;

    Ok(())
}

pub(super) async fn record_creation_keep_last_modified(
    con: &mut SqliteConnection,
    obj: &S3Object,
) -> Result<(), Report> {
    let last_modified = obj.last_modified.as_millisecond();
    let size = obj.size as i64;

    query!(
        r#"
        INSERT INTO objects (bucket, key, assigned_upstream, last_modified, last_accessed, size)
        VALUES ($1, $2, $3, $4, $4, $5)
        ON CONFLICT (bucket, key) DO UPDATE SET
            assigned_upstream = excluded.assigned_upstream,
            size = excluded.size
        "#,
        obj.id.bucket,
        obj.id.key,
        obj.assigned_upstream,
        last_modified,
        size
    )
    .execute(con)
    .await
    .context("failed to record object creation")
    .attach(format!("object: {}", obj.id))
    .attach(format!("upstream: {}", obj.assigned_upstream))?;

    Ok(())
}

pub(super) async fn set_upstream(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
    upstream: &UpstreamId,
) -> Result<(), Report> {
    sqlx::query!(
        r#"
        UPDATE objects
        SET assigned_upstream = ?
        WHERE bucket = ? AND key = ?
        "#,
        upstream,
        obj.bucket,
        obj.key
    )
    .execute(con)
    .await
    .context("failed to set upstream for object")
    .attach(format!("upstream: {upstream}"))
    .attach(format!("object: '{}'", obj))?;

    Ok(())
}

pub(super) async fn get_all_buckets(
    con: &mut SqliteConnection,
) -> Result<std::collections::HashSet<String>, Report> {
    let rows = query!("SELECT DISTINCT bucket FROM objects")
        .fetch_all(con)
        .await
        .context("failed to fetch buckets")?;

    Ok(rows.into_iter().map(|row| row.bucket).collect())
}

pub(super) async fn get_pending_migrations_for_rule(
    con: &mut SqliteConnection,
    rule: &TieringRule,
    now: &Zoned,
) -> Result<Vec<PendingMigration>, Report> {
    let rule_query = tier_rule::to_sql(rule.filter.clone(), now);

    let sql = format!("SELECT * FROM Objects WHERE {}", &rule_query.condition,);
    let mut query = query_as::<Sqlite, DbObject>(&sql);
    for arg in &rule_query.arguments {
        query = match arg {
            SqlArgument::String(str) => query.bind(str),
            SqlArgument::Number(num) => query.bind(num),
            SqlArgument::TimeSpan(num) => query.bind(num * 1000),
            SqlArgument::Bool(b) => query.bind(b),
        };
    }

    let migrations: Vec<PendingMigration> = query
        .fetch_all(con)
        .await
        .context("failed to get objects for rule")?
        .into_iter()
        .map(|obj| PendingMigration {
            object: S3ObjectId {
                bucket: obj.bucket,
                key: obj.key,
            },
            source_upstream: obj.assigned_upstream,
            target_upstream: rule.to.clone(),
        })
        .collect();

    Ok(migrations)
}

pub(super) async fn get_num_of_objects_without_size(
    con: &mut SqliteConnection,
) -> Result<HashMap<String, usize>, Report> {
    Ok(query!(
        r#"
        SELECT
            bucket,
            COUNT(*) AS count
        FROM objects
        WHERE size = 46179488366592
        GROUP BY bucket
        HAVING COUNT(*) > 0
        "#
    )
    .map(|it| (it.bucket, it.count as usize))
    .fetch_all(con)
    .await
    .context("failed to count objects without size")?
    .into_iter()
    .collect())
}
