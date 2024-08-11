pub(crate) mod component;
pub(crate) mod server;
pub(crate) mod execution;

#[allow(clippy::too_many_lines)]
#[allow(clippy::default_trait_access)]
#[allow(clippy::struct_field_names)]
#[allow(clippy::similar_names)]
#[allow(clippy::wildcard_imports)]
pub(crate) mod grpc {
    tonic::include_proto!("obelisk");
}
