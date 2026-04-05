use crate::data::{PendingMigration, S3Object, S3ObjectId, TieringRule, UpstreamId};
use crate::policy::expr::{Env, Type};
use crate::policy::tier_rule::SqlArgument;
use jiff::{Timestamp, Zoned};
use rootcause::Report;
use rootcause::prelude::ResultExt;
use sqlx::{FromRow, Sqlite, SqliteConnection, query, query_as};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TierRuleEnv;

impl TierRuleEnv {
    pub fn synthesize_variable_sql(name: &str, now_var: &str) -> String {
        match name {
            "age" => format!("({now_var} - last_modified)"),
            "bucket" => "bucket".to_string(),
            "object" => "(bucket || '/' || key)".to_string(),
            "key" => "key".to_string(),
            "upstream" => "assigned_upstream".to_string(),
            "size" => todo!(),
            _ => unreachable!("Unknown variable: {}", name),
        }
    }

    pub fn format_now(now: &Zoned) -> i64 {
        now.timestamp().as_millisecond()
    }
}

impl Env for TierRuleEnv {
    fn get_var(name: &str) -> Option<Type> {
        match name {
            "age" => Some(Type::Number),
            "bucket" => Some(Type::String),
            "object" => Some(Type::String),
            "key" => Some(Type::String),
            "upstream" => Some(Type::String),
            "size" => Some(Type::Number),
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
        })
    }
}

pub(super) async fn get_object(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
) -> Result<S3Object, Report> {
    query_as!(
        DbObject,
        "SELECT * FROM objects WHERE bucket = $1 AND key = $2",
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

    query!(
        r#"
        INSERT INTO objects (bucket, key, assigned_upstream, last_modified)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (bucket, key) DO UPDATE SET
            assigned_upstream = excluded.assigned_upstream,
            last_modified = excluded.last_modified
        "#,
        obj.id.bucket,
        obj.id.key,
        obj.assigned_upstream,
        last_modified
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
) -> Result<Vec<PendingMigration>, Report> {
    let sql = format!("SELECT * FROM Objects WHERE {}", &rule.query.condition);
    let mut query = query_as::<Sqlite, DbObject>(&sql);
    for arg in &rule.query.arguments {
        query = match arg {
            SqlArgument::String(str) => query.bind(str),
            SqlArgument::Number(num) => query.bind(num),
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
