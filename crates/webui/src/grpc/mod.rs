use grpc_client::result_detail;

pub mod execution_id;
pub mod ffqn;
pub mod grpc_client;
pub mod join_set_id;

pub trait ResultValueExt {
    fn is_ok(&self) -> bool;
    fn is_err(&self) -> bool;
}

impl ResultValueExt for result_detail::Value {
    fn is_ok(&self) -> bool {
        matches!(self, result_detail::Value::Ok(_))
    }

    fn is_err(&self) -> bool {
        !self.is_ok()
    }
}
