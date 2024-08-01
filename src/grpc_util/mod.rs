pub(crate) mod grpc_mapping;

pub(crate) type TonicResult<T> = Result<T, tonic::Status>;

pub(crate) type TonicRespResult<T> = TonicResult<tonic::Response<T>>;
