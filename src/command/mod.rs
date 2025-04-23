pub(crate) mod component;
pub(crate) mod execution;
pub(crate) mod generate;
pub(crate) mod server;

// TODO: move to parent module
#[allow(clippy::too_many_lines)]
#[allow(clippy::default_trait_access)]
#[allow(clippy::struct_field_names)]
#[allow(clippy::similar_names)]
#[allow(clippy::wildcard_imports)]
#[allow(clippy::doc_markdown)]
pub(crate) mod grpc {
    tonic::include_proto!("obelisk");
}
