use bincode;
use data_type::DataType;
use error::{Error, Result};
use internal_types::TupleData;
use nom_sql::{
    ArithmeticBase, ArithmeticExpression, ArithmeticOperator, Column,
    ConditionBase, ConditionExpression, ConditionTree, Literal, Operator,
};
use std::cmp::{Ord, Ordering};
use std::ops::{Add, Div, Mul, Sub};
use index::IndexType;
use tuple::TupleDesc;
use rel::Rel;

#[cfg(test)]
mod tests;

#[macro_use]
macro_rules! arithmetic_op {
    ($left:expr, $right:expr, $data:expr, $t:ty, $op:ident) => {{
        let l = bincode::deserialize::<$t>(&($left.function)($data)?)?;
        let r = bincode::deserialize::<$t>(&($right.function)($data)?)?;
        Ok(bincode::serialize(&(l.$op(r)))?)
    }};
}

#[macro_use]
macro_rules! arithmetic_expr {
    ($left:expr, $right:expr, $op:ident) => {
        Expr {
            output_type: $left.output_type,
            function: Box::new(move |bytes| {
                match (&$left.output_type, &$right.output_type) {
                    (&DataType::I32, &DataType::I32) => arithmetic_op!($left, $right, bytes, i32, $op),
                    (&DataType::I64, &DataType::I64) => arithmetic_op!($left, $right, bytes, i64, $op),
                    (&DataType::U32, &DataType::U32) => arithmetic_op!($left, $right, bytes, u32, $op),
                    (&DataType::U64, &DataType::U64) => arithmetic_op!($left, $right, bytes, u64, $op),
                    _ => panic!(
                        "Cannot do arithmetic operation with types {:?} and {:?}",
                        $left.output_type, $right.output_type
                    ),
                }
            }),
        }
    };
}

#[macro_use]
macro_rules! cmp_op {
    ($left:expr, $right:expr, $data:expr, $t:ty, $ord:pat) => {{
        let l = bincode::deserialize::<$t>(&($left.function)($data)?)?;
        let r = bincode::deserialize::<$t>(&($right.function)($data)?)?;
        match l.cmp(&r) {
            $ord => Ok(vec![1u8]),
            _ => Ok(vec![0u8]),
        }
    }};
}

#[macro_use]
macro_rules! cmp_expr {
    ($left:expr, $right:expr, $ord:pat) => {
        Expr {
            output_type: DataType::Bool,
            function: Box::new(move |bytes| {
                match (&$left.output_type, &$right.output_type) {
                    (&DataType::I32, &DataType::I32) => {
                        cmp_op!($left, $right, bytes, i32, $ord)
                    }
                    (&DataType::I64, &DataType::I64) => {
                        cmp_op!($left, $right, bytes, i64, $ord)
                    }
                    (&DataType::U32, &DataType::U32) => {
                        cmp_op!($left, $right, bytes, u32, $ord)
                    }
                    (&DataType::U64, &DataType::U64) => {
                        cmp_op!($left, $right, bytes, u64, $ord)
                    }
                    (&DataType::VarChar, &DataType::VarChar) => {
                        cmp_op!($left, $right, bytes, String, $ord)
                    }
                    (&DataType::Char, &DataType::Char)
                    | (&DataType::Bool, &DataType::Bool) => {
                        cmp_op!($left, $right, bytes, u8, $ord)
                    }
                    _ => panic!(
                        "Cannot do comparison between types {:?} and {:?}",
                        $left.output_type, $right.output_type
                    ),
                }
            }),
        }
    };
}

pub struct Expr {
    pub function: Box<dyn Fn(&[u8]) -> Result<TupleData>>,
    pub output_type: DataType,
}

impl Expr {
    pub fn from_nom<E>(nom: E, rel: &Rel) -> Result<Expr>
    where
        E: Into<NomExpr>,
    {
        let nom: NomExpr = nom.into();
        let not_impl = Error::internal("Not implemented");

        match nom {
            NomExpr::ConditionTree(expr) => {
                let indices = rel.indices();
                let left = Expr::from_nom((*expr.left).clone(), rel)?;
                let right = Expr::from_nom((*expr.right).clone(), rel)?;
                let (left, right) = Expr::try_match_type(left, right)?;
                match expr.operator {
                    Operator::Equal => {
                        Ok(cmp_expr!(left, right, Ordering::Equal))
                    }
                    Operator::NotEqual => {
                        let eq_expr = cmp_expr!(left, right, Ordering::Equal);
                        eq_expr.negate()
                    }
                    _ => Err(not_impl),
                }
            }

            NomExpr::ConditionExpression(expr) => match expr {
                ConditionExpression::ComparisonOp(expr) => {
                    Expr::from_nom(expr, rel)
                }
                ConditionExpression::LogicalOp(expr) => {
                    Expr::from_nom(expr, rel)
                }
                ConditionExpression::NegationOp(expr) => {
                    Expr::from_nom((*expr).clone(), rel)
                }
                ConditionExpression::Base(expr) => Expr::from_nom(expr, rel),
                ConditionExpression::Arithmetic(expr) => {
                    Expr::from_nom((*expr).clone(), rel)
                }
                ConditionExpression::Bracketed(expr) => {
                    Expr::from_nom((*expr).clone(), rel)
                }
            }

            NomExpr::ConditionBase(expr) => match expr {
                ConditionBase::Field(col) => Expr::from_col(col, rel.tuple_desc()),
                ConditionBase::Literal(literal) => Expr::from_literal(literal),
                _ => Err(not_impl),
            }

            NomExpr::ArithmeticExpression(expr) => {
                let left = Expr::from_nom(expr.left.clone(), rel)?;
                let right = Expr::from_nom(expr.right.clone(), rel)?;
                if !left.output_type.is_numerical()
                    || !right.output_type.is_numerical()
                {
                    return Err(Error::internal("Arithmetic expression must be between 2 numerical types"));
                }
                let (left, right) = Expr::match_numerical_type(left, right)?;
                match expr.op {
                    ArithmeticOperator::Add => {
                        Ok(arithmetic_expr!(left, right, add))
                    }
                    ArithmeticOperator::Subtract => {
                        Ok(arithmetic_expr!(left, right, sub))
                    }
                    ArithmeticOperator::Multiply => {
                        Ok(arithmetic_expr!(left, right, mul))
                    }
                    ArithmeticOperator::Divide => {
                        Ok(arithmetic_expr!(left, right, div))
                    }
                }
            }

            NomExpr::ArithmeticBase(expr) => match expr {
                ArithmeticBase::Column(col) => Expr::from_col(col, rel.tuple_desc()),
                ArithmeticBase::Scalar(literal) => Expr::from_literal(literal),
            }

        }
    }

    fn from_col(col: Column, desc: TupleDesc) -> Result<Expr> {
        match desc.attr_index(&col.name) {
            Some(idx) => {
                let output_type = desc.attr_types()[idx].clone();
                let function = Box::new(move |bytes: &[u8]| {
                    Ok(desc.cols(bytes)?[idx].clone())
                });
                Ok(Expr {
                    function,
                    output_type,
                })
            }
            None => Err(Error::internal(format!(
                "Invalid column {}",
                col.name
            ))),
        }
    }

    fn from_literal(literal: Literal) -> Result<Expr> {
        match literal {
            Literal::Integer(int) => {
                let data = bincode::serialize(&int)?;
                Ok(Expr {
                    function: Box::new(move |_| Ok(data.clone())),
                    output_type: DataType::I64,
                })
            }
            Literal::String(string) => {
                let data = bincode::serialize(&string)?;
                Ok(Expr {
                    function: Box::new(move |_| Ok(data.clone())),
                    output_type: DataType::VarChar,
                })
            }
            _ => Err(Error::internal("Literal type not supported yet")),
        }
    }

    pub fn is_only_col<E>(nom: E, rel: &Rel) -> Option<usize>
    where
        E: Into<NomExpr>,
    {
        let nom: NomExpr = nom.into();
        match nom {
            NomExpr::ConditionExpression(expr) => match expr {
                ConditionExpression::Base(expr) => Expr::is_only_col(expr, rel),
                _ => None
            }

            NomExpr::ConditionBase(expr) => match expr {
                ConditionBase::Field(col) => {
                    rel.tuple_desc().attr_index(&col.name)
                }
                _ => None
            }

            _ => None
        }
    }

    pub fn is_no_col<E>(nom: E) -> bool
    where
        E: Into<NomExpr>,
    {
        let nom: NomExpr = nom.into();
        match nom {
            NomExpr::ConditionTree(expr) => {
                Expr::is_no_col((*expr.left).clone()) &&
                    Expr::is_no_col((*expr.right).clone())
            }

            NomExpr::ConditionExpression(expr) => match expr {
                ConditionExpression::ComparisonOp(expr) => {
                    Expr::is_no_col(expr)
                }
                ConditionExpression::LogicalOp(expr) => {
                    Expr::is_no_col(expr)
                }
                ConditionExpression::NegationOp(expr) => {
                    Expr::is_no_col((*expr).clone())
                }
                ConditionExpression::Base(expr) => Expr::is_no_col(expr),
                ConditionExpression::Arithmetic(expr) => {
                    Expr::is_no_col((*expr).clone())
                }
                ConditionExpression::Bracketed(expr) => {
                    Expr::is_no_col((*expr).clone())
                }
            }

            NomExpr::ConditionBase(expr) => match expr {
                ConditionBase::Field(_) => false,
                _ => true,
            }

            NomExpr::ArithmeticExpression(expr) => {
                Expr::is_no_col(expr.left.clone()) &&
                    Expr::is_no_col(expr.right.clone())
            }

            NomExpr::ArithmeticBase(expr) => match expr {
                ArithmeticBase::Column(_) => false,
                _ => true,
            }
        }
    }

    fn negate(self) -> Result<Expr> {
        if self.output_type != DataType::Bool {
            Err(Error::internal(format!(
                "Cannot negate {:?}",
                self.output_type
            )))
        } else {
            Ok(Expr {
                output_type: DataType::Bool,
                function: Box::new(move |bytes| {
                    let mut value: u8 =
                        bincode::deserialize(&(self.function)(bytes)?)?;
                    value = match value {
                        0 => 1,
                        _ => 0,
                    };
                    Ok(bincode::serialize(&value)?)
                }),
            })
        }
    }

    fn try_match_type(left: Expr, right: Expr) -> Result<(Expr, Expr)> {
        if left.output_type == right.output_type {
            Ok((left, right))
        } else if left.output_type.is_numerical()
            && right.output_type.is_numerical()
        {
            Expr::match_numerical_type(left, right)
        } else {
            match (&left.output_type, &right.output_type) {
                (&DataType::VarChar, _) | (_, &DataType::VarChar) => Ok((
                    left.cast(DataType::VarChar)?,
                    right.cast(DataType::VarChar)?,
                )),
                _ => Ok((left, right)),
            }
        }
    }

    fn match_numerical_type(left: Expr, right: Expr) -> Result<(Expr, Expr)> {
        if !left.output_type.is_numerical() | !right.output_type.is_numerical()
        {
            Err(Error::internal(format!("Cannot match numerical types")))
        } else {
            let lsize = left.output_type.data_size(None)?;
            let rsize = right.output_type.data_size(None)?;
            let cast_to = if lsize > rsize {
                left.output_type
            } else if lsize < rsize {
                right.output_type
            } else if lsize == 4 {
                DataType::I32
            } else if lsize == 8 {
                DataType::I64
            } else {
                panic!(
                    "Failed to match {:?} and {:?}",
                    left.output_type, right.output_type
                );
            };

            Ok((left.cast(cast_to)?, right.cast(cast_to)?))
        }
    }

    fn cast(self, to: DataType) -> Result<Expr> {
        if self.output_type == to {
            return Ok(self);
        }

        if self.output_type.is_numerical() && to.is_numerical() {
            return Ok(Expr {
                output_type: to,
                function: Box::new(move |bytes| {
                    let mut output = (self.function)(bytes)?;
                    let casted_size = to.data_size(None)?;
                    if output.len() < casted_size {
                        output
                            .append(&mut vec![0u8; casted_size - output.len()]);
                        Ok(output)
                    } else {
                        Ok(output[0..casted_size].to_vec())
                    }
                }),
            });
        }

        match (&self.output_type, &to) {
            (_, &DataType::VarChar) => Ok(Expr {
                output_type: to,
                function: Box::new(move |bytes| {
                    let output = (self.function)(bytes)?;
                    let output_str =
                        self.output_type.data_to_string(&output)?;
                    Ok(bincode::serialize(&output_str)?)
                }),
            }),
            (&DataType::Char, &DataType::Bool)
            | (&DataType::Bool, &DataType::Char) => Ok(Expr {
                output_type: to,
                function: self.function,
            }),
            _ => Err(Error::internal("Not supported")),
        }
    }
}

#[derive(Debug)]
pub enum NomExpr {
    ConditionTree(ConditionTree),
    ConditionExpression(ConditionExpression),
    ConditionBase(ConditionBase),
    ArithmeticExpression(ArithmeticExpression),
    ArithmeticBase(ArithmeticBase),
}

#[macro_use]
macro_rules! impl_from_for_nomexpr {
    ($nom_t: ident) => {
        impl From<$nom_t> for NomExpr {
            fn from(expr: $nom_t) -> Self {
                NomExpr::$nom_t(expr)
            }
        }
    };
}

impl_from_for_nomexpr!(ConditionTree);
impl_from_for_nomexpr!(ConditionExpression);
impl_from_for_nomexpr!(ConditionBase);
impl_from_for_nomexpr!(ArithmeticExpression);
impl_from_for_nomexpr!(ArithmeticBase);
