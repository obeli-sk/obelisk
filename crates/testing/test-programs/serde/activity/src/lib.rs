use crate::generated::exports::testing::serde::serde::Stargazers;
use generated::export;
use generated::exports::testing::serde::serde::{
    Guest, MyError, MyErrorWithPermanent, MyRecord, MyVariant,
};

mod generated {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn rec(my_record: MyRecord) -> Result<MyRecord, ()> {
        Ok(my_record)
    }

    fn var(my_variant: MyVariant) -> Result<MyVariant, ()> {
        Ok(my_variant)
    }

    fn trap() -> Result<(), MyError> {
        panic!()
    }

    fn get_stargazers() -> Result<Stargazers, String> {
        Ok(Stargazers {
            cursor: "cursor".to_string(),
            logins: "logins".to_string(),
        })
    }

    fn permanent_err() -> Result<(), MyErrorWithPermanent> {
        Err(MyErrorWithPermanent::PermanentFailure)
    }
}
