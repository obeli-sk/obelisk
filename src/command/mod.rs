pub(crate) mod component;
pub(crate) mod execution;
pub(crate) mod server; // FIXME: Rename to deamon

pub(crate) mod grpc {
    tonic::include_proto!("obelisk");
}
