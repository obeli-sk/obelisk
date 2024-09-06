pub(crate) mod component;
pub(crate) mod execution;
pub(crate) mod server;

#[expect(clippy::too_many_lines)]
#[expect(clippy::default_trait_access)]
#[expect(clippy::struct_field_names)]
#[expect(clippy::similar_names)]
pub(crate) mod grpc {
    tonic::include_proto!("obelisk");
}
