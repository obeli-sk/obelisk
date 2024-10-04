use indexmap::indexmap;
use type_wrapper::TypeWrapper;

mod core;
pub mod type_wrapper;
pub mod wast_val;
pub mod wast_val_ser;

pub fn execution_error_tuple() -> Option<Box<TypeWrapper>> {
    Some(Box::new(TypeWrapper::Tuple(vec![
        TypeWrapper::String, // execution id
        TypeWrapper::Variant(indexmap! {
            Box::from("permanent-failure") => Some(TypeWrapper::String),
            Box::from("permanent-timeout") => None,
            Box::from("non-determinism") => None,
        }),
    ])))
}
