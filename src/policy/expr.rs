use ariadne::{Color, Label, ReportKind, Source};
use chumsky::pratt::*;
use chumsky::prelude::*;
use chumsky::text::TextExpected;
use derive_more::Display;
use rootcause::{Report, report};
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::ops::Range;

type OurErr<'src> = extra::Err<Rich<'src, char>>;

pub struct AnnotatedError(pub ariadne::Report<'static, ((), Range<usize>)>, String);

impl AnnotatedError {
    pub fn format(&self) -> Result<String, Report> {
        let mut out = Vec::new();
        self.0
            .write_for_stdout(Source::from(&self.1), &mut out)
            .map_err(|_| std::fmt::Error)?;

        Ok(String::from_utf8_lossy(&out).to_string())
    }
}

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq)]
pub enum Type {
    String,
    Number,
    Bool,
}

pub trait Env {
    fn get_var(name: &str) -> Option<Type>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Raw;

#[derive(Debug, Clone)]
pub struct Typechecked<E> {
    typ: Type,
    env: PhantomData<E>,
}

impl<E> Typechecked<E> {
    fn new(typ: Type) -> Self {
        Self {
            typ,
            env: PhantomData,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Operator<T: Clone> {
    Negate(Box<Spanned<Expr<T>>>),
    Not(Box<Spanned<Expr<T>>>),

    Times(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),
    Divide(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),

    Plus(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),
    Minus(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),

    Equal(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),
    RegexEqual(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),
    NotEqual(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),
    RelGt(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),
    RelGe(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),
    RelLt(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),
    RelLe(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),

    And(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),
    Or(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>),
}

impl<T: Clone> Operator<T> {
    pub fn to_debug_string(&self) -> String {
        macro_rules! bin_op {
            ($l:expr, $r:expr, $op:literal) => {
                format!(
                    "({} {} {})",
                    $l.inner.to_debug_string(),
                    $op,
                    $r.inner.to_debug_string()
                )
            };
        }
        match self {
            Self::Negate(val) => format!("-{}", val.inner.to_debug_string()),
            Self::Not(val) => format!("!{}", val.inner.to_debug_string()),
            Self::Times(l, r) => bin_op!(l, r, "*"),
            Self::Divide(l, r) => bin_op!(l, r, "/"),
            Self::Plus(l, r) => bin_op!(l, r, "+"),
            Self::Minus(l, r) => bin_op!(l, r, "-"),
            Self::Equal(l, r) => bin_op!(l, r, "=="),
            Self::RegexEqual(l, r) => bin_op!(l, r, "~="),
            Self::NotEqual(l, r) => bin_op!(l, r, "!="),
            Self::RelGt(l, r) => bin_op!(l, r, ">"),
            Self::RelGe(l, r) => bin_op!(l, r, ">="),
            Self::RelLt(l, r) => bin_op!(l, r, "<"),
            Self::RelLe(l, r) => bin_op!(l, r, "<="),
            Self::And(l, r) => bin_op!(l, r, "&&"),
            Self::Or(l, r) => bin_op!(l, r, "||"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr<T: Clone> {
    Variable(String, T),
    Number(i64, T),
    /// Time in seconds
    TimeSpan(i64, T),
    Bool(bool, T),
    String(String, T),
    Operator(Operator<T>, T),
}

impl<T: Clone> Display for Expr<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_debug_string())
    }
}

impl<T: Clone> Expr<T> {
    pub fn data(&self) -> &T {
        match self {
            Self::Variable(_, data) => data,
            Self::Number(_, data) => data,
            Self::TimeSpan(_, data) => data,
            Self::String(_, data) => data,
            Self::Bool(_, data) => data,
            Self::Operator(_, data) => data,
        }
    }

    pub fn to_debug_string(&self) -> String {
        match self {
            Self::Variable(var, _) => var.clone(),
            Self::Number(num, _) => num.to_string(),
            Self::TimeSpan(secs, _) => {
                // subdivide out days, hours, minutes, seconds
                let days = secs / 86400;
                let hours = (secs % 86400) / 3600;
                let mins = (secs % 3600) / 60;
                let secs = secs % 60;
                let mut s = String::new();
                if days > 0 {
                    s += &days.to_string();
                    s += "d";
                }
                if hours > 0 {
                    s += &hours.to_string();
                    s += "h";
                }
                if mins > 0 {
                    s += &mins.to_string();
                    s += "m";
                }
                if secs > 0 {
                    s += &secs.to_string();
                    s += "s";
                }
                if s.is_empty() {
                    s += "0s";
                }
                s
            }
            Self::String(str, _) => format!("\'{}\'", str),
            Self::Bool(bool, _) => bool.to_string(),
            Self::Operator(op, _) => op.to_debug_string(),
        }
    }
}

fn p_int<'src>() -> impl Parser<'src, &'src str, i64, OurErr<'src>> + Clone {
    // Reimplement text::int to allow '_' separators
    just('-')
        .or_not()
        .then(
            any()
                .filter(move |c: &char| c.is_ascii_digit() && *c != '0')
                .then(
                    any()
                        .filter(move |c: &char| c.is_ascii_digit() || *c == '_')
                        .repeated(),
                )
                .ignored()
                .or(just('0').ignored())
                .to_slice()
                .labelled_with(|| TextExpected::<&str>::Int),
        )
        .try_map(|(sign, num_str): (Option<char>, &str), span| {
            num_str
                .replace("_", "")
                .parse::<i64>()
                .map(|n| if sign.is_some() { -n } else { n })
                .map_err(|e| Rich::custom(span, format!("invalid integer: {e}")))
        })
}

fn p_time_span<'src>() -> impl Parser<'src, &'src str, i64, OurErr<'src>> + Clone {
    let p_suffix = choice((
        p_int().then_ignore(just('d')).map(|it| it * 60 * 60 * 24),
        p_int().then_ignore(just('h')).map(|it| it * 60 * 60),
        p_int().then_ignore(just('m')).map(|it| it * 60),
        p_int().then_ignore(just('s')).map(|it| it),
    ));
    p_suffix
        .repeated()
        .at_least(1)
        .collect::<Vec<i64>>()
        .map(|parts| parts.iter().sum())
}

fn p_string<'src>() -> impl Parser<'src, &'src str, String, OurErr<'src>> + Clone {
    let esc = just('\\').ignore_then(any());
    choice((esc, any().filter(|it: &char| *it != '\'')))
        .repeated()
        .collect::<String>()
        .padded_by(just("'"))
}

fn p_bool<'src>() -> impl Parser<'src, &'src str, bool, OurErr<'src>> + Clone {
    just("true").map(|_| true).or(just("false").map(|_| false))
}

fn p_var<'src>() -> impl Parser<'src, &'src str, Expr<Raw>, OurErr<'src>> + Clone {
    text::ident()
        .map(String::from)
        .map(|it| Expr::Variable(it, Raw))
}

fn p_atom<'src>() -> impl Parser<'src, &'src str, Expr<Raw>, OurErr<'src>> + Clone {
    p_time_span()
        .map(|it| Expr::TimeSpan(it, Raw))
        .or(p_int().map(|it| Expr::Number(it, Raw)))
        .or(p_bool().map(|it| Expr::Bool(it, Raw)))
        .or(p_string().map(|it| Expr::String(it, Raw)))
        .or(p_var())
}

fn p_expr<'src>() -> impl Parser<'src, &'src str, Spanned<Expr<Raw>>, OurErr<'src>> + Clone {
    macro_rules! infix {
        ($precedence:expr, $op:literal, $operator:ident) => {
            infix(
                $precedence,
                just($op).padded(),
                |l: Spanned<Expr<Raw>>, _, r: Spanned<Expr<Raw>>, _| {
                    let span = l.span.union(r.span);
                    Spanned {
                        inner: Expr::Operator(Operator::$operator(Box::new(l), Box::new(r)), Raw),
                        span,
                    }
                },
            )
        };
    }
    macro_rules! prefix {
        ($precedence:literal, $op:literal, $operator:ident) => {
            prefix(
                $precedence,
                just($op).padded(),
                |_, rhs: Spanned<Expr<Raw>>, _| {
                    let span = rhs.span;
                    Spanned {
                        inner: Expr::Operator(Operator::$operator(Box::new(rhs)), Raw),
                        span,
                    }
                },
            )
        };
    }
    recursive(|expr| {
        p_atom()
            .spanned()
            .or(expr.delimited_by(just('(').padded(), just(')').padded()))
            .pratt((
                // unary prefix
                prefix!(7, "-", Negate),
                prefix!(7, "!", Not),
                // multiplicative
                infix!(left(6), "*", Times),
                infix!(left(6), "/", Divide),
                // additive
                infix!(left(5), "+", Plus),
                infix!(left(5), "-", Minus),
                // relational
                infix!(left(4), "==", Equal),
                infix!(left(4), "~=", RegexEqual),
                infix!(left(4), "!=", NotEqual),
                infix!(left(4), ">", RelGt),
                infix!(left(4), ">=", RelGe),
                infix!(left(4), "<", RelLt),
                infix!(left(4), "<=", RelLe),
                // logical and
                infix!(left(3), "&&", And),
                // logical or
                infix!(left(2), "||", Or),
            ))
    })
}

pub fn parse_expr(inp: &str) -> Result<Spanned<Expr<Raw>>, Report<AnnotatedError>> {
    let result = p_expr().parse(inp);

    match result.into_result() {
        Ok(val) => Ok(val),
        Err(errors) => {
            // Only report the first error
            let error = errors.into_iter().next().unwrap();
            Err(report!(AnnotatedError(
                ariadne::Report::build(ReportKind::Error, ((), error.span().into_range()))
                    .with_config(ariadne::Config::new().with_index_type(ariadne::IndexType::Byte))
                    .with_message("Parsing error")
                    .with_label(
                        Label::new(((), error.span().into_range()))
                            .with_message(error.to_string())
                            .with_color(Color::Red),
                    )
                    .finish(),
                inp.to_string()
            )))
        }
    }
}

pub fn typecheck<E: Env + Clone>(
    expr: Spanned<Expr<Raw>>,
    src: &str,
) -> Result<Expr<Typechecked<E>>, Report<AnnotatedError>> {
    let check_inner = |e| map_expr(e, |it| typecheck(it, src));
    macro_rules! bin_op {
        ($l:expr, $r:expr, $op:ident, $l_ty:ident, $r_ty:ident, $res_ty:ident) => {
            Expr::Operator(
                Operator::$op(
                    check_inner($l)?.assert_typ(expr.span, src, Type::$l_ty)?,
                    check_inner($r)?.assert_typ(expr.span, src, Type::$r_ty)?,
                ),
                Typechecked::new(Type::$res_ty),
            )
        };
    }
    macro_rules! equal_op {
        ($l:expr, $r:expr, $op:ident) => {{
            let l = check_inner($l)?;
            let r = check_inner($r)?;
            if l.typ() != r.typ() {
                return Err(report!(AnnotatedError(
                    ariadne::Report::build(ReportKind::Error, ((), expr.span.into_range()))
                        .with_config(
                            ariadne::Config::new().with_index_type(ariadne::IndexType::Byte)
                        )
                        .with_message("Unknown variable")
                        .with_label(
                            Label::new(((), r.span.into_range()))
                                .with_message(format!("type {}", l.typ()))
                                .with_color(Color::Red),
                        )
                        .with_label(
                            Label::new(((), r.span.into_range()))
                                .with_message(format!("type {}", r.typ()))
                                .with_color(Color::Red),
                        )
                        .finish(),
                    src.to_string()
                )));
            }
            Expr::Operator(Operator::$op(l, r), Typechecked::new(Type::Bool))
        }};
    }

    Ok(match expr.inner {
        Expr::Variable(var, _) => {
            let Some(typ) = E::get_var(&var) else {
                return Err(report!(AnnotatedError(
                    ariadne::Report::build(ReportKind::Error, ((), expr.span.into_range()))
                        .with_config(
                            ariadne::Config::new().with_index_type(ariadne::IndexType::Byte)
                        )
                        .with_message("Unknown variable")
                        .with_label(
                            Label::new(((), expr.span.into_range()))
                                .with_message(format!("no variable named '{var}'",))
                                .with_color(Color::Red),
                        )
                        .finish(),
                    src.to_string()
                )));
            };
            Expr::Variable(var, Typechecked::new(typ))
        }
        Expr::Number(num, _) => Expr::Number(num, Typechecked::new(Type::Number)),
        Expr::TimeSpan(num, _) => Expr::TimeSpan(num, Typechecked::new(Type::Number)),
        Expr::String(s, _) => Expr::String(s, Typechecked::new(Type::String)),
        Expr::Bool(b, _) => Expr::Bool(b, Typechecked::new(Type::Bool)),
        Expr::Operator(op, _) => match op {
            Operator::Negate(r) => Expr::Operator(
                Operator::Negate(check_inner(r)?.assert_typ(expr.span, src, Type::Number)?),
                Typechecked::new(Type::Number),
            ),
            Operator::Not(r) => Expr::Operator(
                Operator::Negate(check_inner(r)?.assert_typ(expr.span, src, Type::Bool)?),
                Typechecked::new(Type::Bool),
            ),
            Operator::Times(l, r) => bin_op!(l, r, Times, Number, Number, Number),
            Operator::Divide(l, r) => bin_op!(l, r, Divide, Number, Number, Number),
            Operator::Plus(l, r) => bin_op!(l, r, Plus, Number, Number, Number),
            Operator::Minus(l, r) => bin_op!(l, r, Minus, Number, Number, Number),
            Operator::Equal(l, r) => equal_op!(l, r, Equal),
            Operator::RegexEqual(l, r) => bin_op!(l, r, RegexEqual, String, String, Bool),
            Operator::NotEqual(l, r) => equal_op!(l, r, NotEqual),
            Operator::RelGt(l, r) => bin_op!(l, r, RelGt, Number, Number, Bool),
            Operator::RelGe(l, r) => bin_op!(l, r, RelGe, Number, Number, Bool),
            Operator::RelLt(l, r) => bin_op!(l, r, RelLt, Number, Number, Bool),
            Operator::RelLe(l, r) => bin_op!(l, r, RelLe, Number, Number, Bool),
            Operator::Or(l, r) => bin_op!(l, r, Or, Bool, Bool, Bool),
            Operator::And(l, r) => bin_op!(l, r, And, Bool, Bool, Bool),
        },
    })
}

fn map_expr<R, E: Clone>(
    #[allow(clippy::boxed_local)] // this is the signature we need
    expr: Box<Spanned<Expr<Raw>>>,
    f: impl Fn(Spanned<Expr<Raw>>) -> Result<Expr<Typechecked<E>>, R>,
) -> Result<Box<Spanned<Expr<Typechecked<E>>>>, R> {
    let span = expr.span;
    Ok(Box::new(Spanned {
        inner: f(*expr)?,
        span,
    }))
}

trait Typeable
where
    Self: Sized,
{
    fn typ(&self) -> Type;
    fn assert_typ(
        self,
        full_span: SimpleSpan,
        src: &str,
        typ: Type,
    ) -> Result<Self, Report<AnnotatedError>>;
}

impl<E: Clone> Typeable for Box<Spanned<Expr<Typechecked<E>>>> {
    fn typ(&self) -> Type {
        self.data().typ
    }

    fn assert_typ(
        self,
        full_span: SimpleSpan,
        src: &str,
        typ: Type,
    ) -> Result<Self, Report<AnnotatedError>> {
        if self.typ() != typ {
            return Err(report!(AnnotatedError(
                ariadne::Report::build(ReportKind::Error, ((), full_span.into_range()))
                    .with_config(ariadne::Config::new().with_index_type(ariadne::IndexType::Byte))
                    .with_message("Invalid type")
                    .with_label(
                        Label::new(((), full_span.into_range()))
                            .with_message("In this expression")
                            .with_color(Color::BrightBlack),
                    )
                    .with_label(
                        Label::new(((), self.span.into_range()))
                            .with_message(format!("Expected type {}, found {}", typ, self.typ()))
                            .with_color(Color::Red),
                    )
                    .finish(),
                src.to_string()
            )));
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::{Expr, Operator, Raw, parse_expr};
    use chumsky::prelude::Spanned;
    use chumsky::span::SimpleSpan;
    use proptest::prelude::*;
    use proptest::string::string_regex;

    const OPERATORS: &[&str] = &[
        "*", "/", "+", "-", "==", "~=", "!=", ">", ">=", "<", "<=", "&&", "||",
    ];

    type BinaryOpCtor<T> = fn(Box<Spanned<Expr<T>>>, Box<Spanned<Expr<T>>>) -> Operator<T>;

    fn identifier_strategy() -> impl Strategy<Value = String> {
        string_regex("[a-zA-Z_][a-zA-Z0-9_]{0,8}").expect("valid identifier regex")
    }

    fn integer_strategy() -> impl Strategy<Value = i64> {
        any::<i64>().prop_filter("i64::MIN is not representable", |n| *n != i64::MIN)
    }

    fn string_content_strategy() -> impl Strategy<Value = String> {
        string_regex("[a-zA-Z0-9 _-]{0,12}").expect("valid string content regex")
    }

    fn string_literal_strategy() -> impl Strategy<Value = String> {
        string_content_strategy().prop_map(|content| format!("'{content}'"))
    }

    fn bool_literal_strategy() -> impl Strategy<Value = String> {
        any::<bool>().prop_map(|value| value.to_string())
    }

    fn time_span_parts_strategy() -> impl Strategy<Value = Vec<(i64, char)>> {
        prop::collection::vec(
            prop_oneof![
                (0_i64..=365).prop_map(|value| (value, 'd')),
                (0_i64..=24 * 31).prop_map(|value| (value, 'h')),
                (0_i64..=60 * 24).prop_map(|value| (value, 'm')),
                (0_i64..=60 * 60).prop_map(|value| (value, 's')),
            ],
            1..=4,
        )
    }

    fn render_time_span(parts: &[(i64, char)]) -> String {
        parts
            .iter()
            .map(|(value, unit)| format!("{value}{unit}"))
            .collect::<String>()
    }

    fn time_span_to_millis(parts: &[(i64, char)]) -> i64 {
        parts
            .iter()
            .map(|(value, unit)| {
                value
                    * match unit {
                        'd' => 60 * 60 * 24,
                        'h' => 60 * 60,
                        'm' => 60,
                        's' => 1,
                        _ => unreachable!("time span strategy only emits known units"),
                    }
            })
            .sum()
    }

    fn time_span_literal_strategy() -> impl Strategy<Value = String> {
        time_span_parts_strategy().prop_map(|parts| render_time_span(&parts))
    }

    fn unary_op_strategy() -> impl Strategy<Value = &'static str> {
        prop_oneof![Just("-"), Just("!")]
    }

    fn binary_op_strategy() -> impl Strategy<Value = &'static str> {
        OPERATORS
            .iter()
            .fold(Just(OPERATORS[0]).boxed(), |acc, &op| {
                acc.prop_union(Just(op).boxed()).boxed()
            })
    }

    fn binary_op_ctor_strategy() -> impl Strategy<Value = BinaryOpCtor<Raw>> {
        prop_oneof![
            Just(Operator::Times as BinaryOpCtor<Raw>),
            Just(Operator::Divide as BinaryOpCtor<Raw>),
            Just(Operator::Plus as BinaryOpCtor<Raw>),
            Just(Operator::Minus as BinaryOpCtor<Raw>),
            Just(Operator::Equal as BinaryOpCtor<Raw>),
            Just(Operator::RegexEqual as BinaryOpCtor<Raw>),
            Just(Operator::NotEqual as BinaryOpCtor<Raw>),
            Just(Operator::RelGt as BinaryOpCtor<Raw>),
            Just(Operator::RelGe as BinaryOpCtor<Raw>),
            Just(Operator::RelLt as BinaryOpCtor<Raw>),
            Just(Operator::RelLe as BinaryOpCtor<Raw>),
            Just(Operator::And as BinaryOpCtor<Raw>),
            Just(Operator::Or as BinaryOpCtor<Raw>),
        ]
    }

    fn expr_strategy() -> impl Strategy<Value = String> {
        let atom = prop_oneof![
            identifier_strategy(),
            integer_strategy().prop_map(|it| it.to_string()),
            string_literal_strategy(),
            bool_literal_strategy(),
            time_span_literal_strategy(),
        ];

        atom.prop_recursive(4, 64, 2, |inner| {
            prop_oneof![
                (unary_op_strategy(), inner.clone()).prop_map(|(op, rhs)| format!("({op}{rhs})")),
                (inner.clone(), binary_op_strategy(), inner.clone())
                    .prop_map(|(lhs, op, rhs)| format!("({lhs} {op} {rhs})")),
            ]
        })
    }

    fn spanned<T: Clone>(inner: Expr<T>) -> Spanned<Expr<T>> {
        Spanned {
            inner,
            span: (0..0).into(),
        }
    }

    fn ast_strategy() -> impl Strategy<Value = Spanned<Expr<Raw>>> {
        let atom = prop_oneof![
            identifier_strategy().prop_map(|name| spanned(Expr::Variable(name, Raw))),
            integer_strategy().prop_map(|number| spanned(Expr::Number(number, Raw))),
            string_content_strategy().prop_map(|value| spanned(Expr::String(value, Raw))),
            any::<bool>().prop_map(|value| spanned(Expr::Bool(value, Raw))),
            time_span_parts_strategy()
                .prop_map(|parts| spanned(Expr::TimeSpan(time_span_to_millis(&parts), Raw))),
        ];

        atom.prop_recursive(4, 128, 3, |inner| {
            prop_oneof![
                inner.clone().prop_map(|rhs| {
                    spanned(Expr::Operator(Operator::Negate(Box::new(rhs)), Raw))
                }),
                inner
                    .clone()
                    .prop_map(|rhs| { spanned(Expr::Operator(Operator::Not(Box::new(rhs)), Raw)) }),
                (inner.clone(), binary_op_ctor_strategy(), inner.clone()).prop_map(
                    |(lhs, op, rhs)| {
                        spanned(Expr::Operator(op(Box::new(lhs), Box::new(rhs)), Raw))
                    }
                )
            ]
        })
    }

    fn normalized_spanned<T: Clone>(expr: Spanned<Expr<T>>) -> Spanned<Expr<T>> {
        Spanned {
            inner: normalized_expr(expr.inner),
            span: SimpleSpan::default(),
        }
    }

    fn normalized_expr<T: Clone>(expr: Expr<T>) -> Expr<T> {
        match expr {
            Expr::Variable(name, data) => Expr::Variable(name.clone(), data),
            Expr::Number(number, data) => Expr::Number(number, data),
            Expr::TimeSpan(number, data) => Expr::TimeSpan(number, data),
            Expr::String(value, data) => Expr::String(value.clone(), data),
            Expr::Bool(value, data) => Expr::Bool(value, data),
            Expr::Operator(Operator::Negate(rhs), data) => {
                let rhs = normalized_spanned(*rhs);
                match rhs.inner {
                    Expr::Number(number, _) => {
                        if let Some(negated) = number.checked_neg() {
                            Expr::Number(negated, data)
                        } else {
                            Expr::Operator(Operator::Negate(Box::new(rhs)), data)
                        }
                    }
                    _ => Expr::Operator(Operator::Negate(Box::new(rhs)), data),
                }
            }
            Expr::Operator(op, data) => Expr::Operator(normalized_operator(op), data),
        }
    }

    fn normalized_operator<T: Clone>(op: Operator<T>) -> Operator<T> {
        #[allow(clippy::boxed_local)] // this is the signature we need
        fn boxed_normalized<T: Clone>(expr: Box<Spanned<Expr<T>>>) -> Box<Spanned<Expr<T>>> {
            Box::new(normalized_spanned(*expr))
        }

        macro_rules! bin {
            ($variant:ident, $lhs:expr, $rhs:expr) => {
                Operator::$variant(boxed_normalized($lhs), boxed_normalized($rhs))
            };
        }

        match op {
            Operator::Negate(rhs) => Operator::Negate(boxed_normalized(rhs)),
            Operator::Not(rhs) => Operator::Not(boxed_normalized(rhs)),
            Operator::Times(lhs, rhs) => bin!(Times, lhs, rhs),
            Operator::Divide(lhs, rhs) => bin!(Divide, lhs, rhs),
            Operator::Plus(lhs, rhs) => bin!(Plus, lhs, rhs),
            Operator::Minus(lhs, rhs) => bin!(Minus, lhs, rhs),
            Operator::Equal(lhs, rhs) => bin!(Equal, lhs, rhs),
            Operator::RegexEqual(lhs, rhs) => bin!(RegexEqual, lhs, rhs),
            Operator::NotEqual(lhs, rhs) => bin!(NotEqual, lhs, rhs),
            Operator::RelGt(lhs, rhs) => bin!(RelGt, lhs, rhs),
            Operator::RelGe(lhs, rhs) => bin!(RelGe, lhs, rhs),
            Operator::RelLt(lhs, rhs) => bin!(RelLt, lhs, rhs),
            Operator::RelLe(lhs, rhs) => bin!(RelLe, lhs, rhs),
            Operator::And(lhs, rhs) => bin!(And, lhs, rhs),
            Operator::Or(lhs, rhs) => bin!(Or, lhs, rhs),
        }
    }

    fn parse_pretty(inp: &str) -> Spanned<Expr<Raw>> {
        match parse_expr(inp) {
            Ok(expr) => expr,
            Err(e) => {
                println!(
                    "{}",
                    e.current_context()
                        .format()
                        .expect("Parsing should succeed")
                );
                panic!("Parsing should succeed");
            }
        }
    }

    proptest! {
        #[test]
        fn parse_expr_accepts_generated_time_spans(span in time_span_literal_strategy()) {
            prop_assert!(
                parse_expr(&span).is_ok(),
                "generated time span failed to parse: {span}"
            );
        }

        #[test]
        fn parse_expr_time_spans_convert_to_millis(parts in time_span_parts_strategy()) {
            let span = render_time_span(&parts);
            let expected = time_span_to_millis(&parts);
            let parsed = parse_pretty(&span);

            prop_assert_eq!(parsed.inner, Expr::TimeSpan(expected, Raw));
        }

        #[test]
        fn parse_expr_accepts_generated_expressions(expr in expr_strategy()) {
            prop_assert!(
                parse_expr(&expr).is_ok(),
                "generated expression failed to parse: {expr}"
            );
        }

        #[test]
        fn parse_expr_roundtrips_via_debug_string(expr in expr_strategy()) {
            let parsed = parse_pretty(&expr);
            let rendered = parsed.inner.to_debug_string();
            let reparsed = parse_pretty(&rendered);

            prop_assert_eq!(reparsed.inner.to_debug_string(), rendered);
            prop_assert_eq!(normalized_expr(parsed.inner), normalized_expr(reparsed.inner));
        }

        #[test]
        fn parse_expr_accepts_surrounding_whitespace(expr in expr_strategy()) {
            let wrapped = format!("({expr})");
            let baseline = parse_pretty(&wrapped);
            let padded = parse_pretty(&format!(" \n\t{wrapped}\t \n "));

            prop_assert_eq!(baseline.inner.to_debug_string(), padded.inner.to_debug_string());
            prop_assert_eq!(normalized_expr(baseline.inner), normalized_expr(padded.inner));
        }

        #[test]
        fn ast_roundtrips_modulo_spans(ast in ast_strategy()) {
            let rendered = ast.inner.to_debug_string();
            let reparsed = parse_pretty(&rendered);

            prop_assert_eq!(normalized_expr(ast.inner), normalized_expr(reparsed.inner));
        }

        #[test]
        fn ast_roundtrip_stays_stable(ast in ast_strategy()) {
            let rendered = ast.inner.to_debug_string();
            let first = parse_pretty(&rendered);
            let second = parse_pretty(&first.inner.to_debug_string());

            // Spans should be identical!
            prop_assert_eq!(first.inner, second.inner);
        }
    }
}
