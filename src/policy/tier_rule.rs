use crate::db::TieringRuleEnv;
use crate::policy::expr::{Expr, Operator, Typechecked};
use jiff::Zoned;

#[derive(Debug, Clone)]
pub enum SqlArgument {
    String(String),
    /// Time in seconds
    TimeSpan(i64),
    Number(i64),
    Bool(bool),
}

struct SqlBuilder {
    arguments: Vec<SqlArgument>,
    now_arg: String,
    arg_offset: usize,
}

impl SqlBuilder {
    fn new(arg_offset: usize, now: &Zoned) -> Self {
        let mut me = Self {
            arguments: Vec::new(),
            now_arg: format!("${}", arg_offset + 1),
            arg_offset,
        };
        me.add_arg(SqlArgument::TimeSpan(now.timestamp().as_second()));
        me
    }

    fn add_arg(&mut self, arg: SqlArgument) -> String {
        self.arguments.push(arg);
        format!("${}", self.arguments.len() + self.arg_offset)
    }

    fn add_expr(&mut self, expr: &Expr<Typechecked<TieringRuleEnv>>) -> String {
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
                TieringRuleEnv::synthesize_variable_sql(var, &self.now_arg).to_string()
            }
            Expr::Number(num, _) => self.add_arg(SqlArgument::Number(*num)),
            Expr::TimeSpan(num, _) => self.add_arg(SqlArgument::TimeSpan(*num)),
            Expr::String(str, _) => self.add_arg(SqlArgument::String(str.clone())),
            Expr::Bool(b, _) => self.add_arg(SqlArgument::Bool(*b)),
            Expr::FunctionCall(name, args, _) => {
                let arg_sql: Vec<String> =
                    args.iter().map(|arg| self.add_expr(&arg.inner)).collect();
                TieringRuleEnv::synthesize_function_sql(name, &self.now_arg, &arg_sql)
            }
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
pub struct TieringRuleQuery {
    pub condition: String,
    pub arguments: Vec<SqlArgument>,
}

pub fn to_sql(
    expr: Expr<Typechecked<TieringRuleEnv>>,
    arg_offset: usize,
    now: &Zoned,
) -> TieringRuleQuery {
    let mut builder = SqlBuilder::new(arg_offset, now);
    let sql = builder.add_expr(&expr);

    TieringRuleQuery {
        condition: sql,
        arguments: builder.arguments,
    }
}
