use crate::policy::expr::{Env, Expr, Operator, Type, Typechecked};
use jiff::Zoned;
use sqlx::query::Query;
use sqlx::{Database, Sqlite};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TierRuleEnv;

impl TierRuleEnv {
    fn synthesize_variable_sql(name: &str, now_var: &str) -> String {
        match name {
            "age" => format!("({now_var} - last_modified)"),
            "bucket" => "bucket".to_string(),
            "object" => "(bucket || '/' || key)".to_string(),
            "key" => "key".to_string(),
            "size" => todo!(),
            _ => unreachable!("Unknown variable: {}", name),
        }
    }

    fn format_now(now: &Zoned) -> i64 {
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
            "size" => Some(Type::Number),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SqlArgument {
    String(String),
    Number(i64),
}

struct SqlBuilder {
    arguments: Vec<SqlArgument>,
    now_arg: String,
}

impl SqlBuilder {
    fn new(now: &Zoned) -> Self {
        let mut me = Self {
            arguments: Vec::new(),
            now_arg: "$1".to_string(),
        };
        me.add_arg(SqlArgument::Number(TierRuleEnv::format_now(now)));
        me
    }

    fn add_arg(&mut self, arg: SqlArgument) -> String {
        self.arguments.push(arg);
        format!("${}", self.arguments.len())
    }

    fn add_expr(&mut self, expr: &Expr<Typechecked<TierRuleEnv>>) -> String {
        macro_rules! bin {
            ($l:expr, $r:expr, $op:literal) => {
                format!(
                    "({} {} {})",
                    self.add_expr(&$l.inner),
                    $op,
                    self.add_expr(&$r.inner)
                )
            };
        }
        match expr {
            Expr::Variable(var, _) => {
                TierRuleEnv::synthesize_variable_sql(var, &self.now_arg).to_string()
            }
            Expr::Number(num, _) => self.add_arg(SqlArgument::Number(*num)),
            Expr::String(str, _) => self.add_arg(SqlArgument::String(str.clone())),
            Expr::Operator(op, _) => match op {
                Operator::Negate(inner) => format!("(-{})", self.add_expr(&inner.inner)),
                Operator::Not(inner) => format!("(NOT {})", self.add_expr(&inner.inner)),
                Operator::Times(l, r) => bin!(l, r, "*"),
                Operator::Divide(l, r) => bin!(l, r, "/"),
                Operator::Plus(l, r) => bin!(l, r, "+"),
                Operator::Minus(l, r) => bin!(l, r, "-"),
                Operator::Equal(l, r) => bin!(l, r, "="),
                Operator::RegexEqual(l, r) => bin!(l, r, "REGEXP"),
                Operator::NotEqual(l, r) => bin!(l, r, "!="),
                Operator::RelGt(l, r) => bin!(l, r, ">"),
                Operator::RelGe(l, r) => bin!(l, r, ">="),
                Operator::RelLt(l, r) => bin!(l, r, "<"),
                Operator::RelLe(l, r) => bin!(l, r, "<="),
                Operator::And(l, r) => bin!(l, r, "AND"),
                Operator::Or(l, r) => bin!(l, r, "OR"),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqlQuery {
    sql: String,
    arguments: Vec<SqlArgument>,
}

impl SqlQuery {
    pub fn to_where_clause(&self) -> &str {
        &self.sql
    }

    #[allow(single_use_lifetimes)] // I do not see a way around it?
    pub fn bind<'query, 'slf: 'query>(
        &'slf self,
        mut query: Query<'query, Sqlite, <Sqlite as Database>::Arguments<'query>>,
    ) -> Query<'query, Sqlite, <Sqlite as Database>::Arguments<'query>> {
        for arg in &self.arguments {
            query = match arg {
                SqlArgument::String(str) => query.bind(str),
                SqlArgument::Number(num) => query.bind(num),
            };
        }
        query
    }
}

pub fn to_sql(expr: Expr<Typechecked<TierRuleEnv>>, now: &Zoned) -> SqlQuery {
    let mut builder = SqlBuilder::new(now);
    let sql = builder.add_expr(&expr);

    SqlQuery {
        sql,
        arguments: builder.arguments,
    }
}
