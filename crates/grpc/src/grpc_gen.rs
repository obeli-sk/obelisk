#![allow(clippy::default_trait_access)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::similar_names)]
#![allow(clippy::wildcard_imports)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::must_use_candidate)]
tonic::include_proto!("obelisk");

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("descriptor");
