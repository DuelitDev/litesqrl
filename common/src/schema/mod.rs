#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Nil,
    Int,
    Real,
    Bool,
    Text,
}

pub enum DataValue {
    Nil,
    Int(i64),
    Real(f64),
    Bool(bool),
    Text(Box<str>),
}
