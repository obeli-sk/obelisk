use super::wasi::filesystem::{preopens, types};
use crate::workflow::workflow_ctx::WorkflowCtx;
use wasmtime::Result;
use wasmtime::component::Resource;
use wasmtime_wasi_io::streams::{DynInputStream, DynOutputStream};

impl preopens::Host for WorkflowCtx {
    fn get_directories(&mut self) -> Result<Vec<(Resource<types::Descriptor>, String)>> {
        // Never construct a Descriptor, so all of the methods in the rest of Filesystem should be
        // unreachable.
        Ok(Vec::new())
    }
}

impl types::HostDescriptor for WorkflowCtx {
    fn read_via_stream(
        &mut self,
        _: Resource<types::Descriptor>,
        _: u64,
    ) -> Result<Result<Resource<DynInputStream>, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn write_via_stream(
        &mut self,
        _: Resource<types::Descriptor>,
        _: u64,
    ) -> Result<Result<Resource<DynOutputStream>, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn append_via_stream(
        &mut self,
        _: Resource<types::Descriptor>,
    ) -> Result<Result<Resource<DynOutputStream>, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn advise(
        &mut self,
        _: Resource<types::Descriptor>,
        _: u64,
        _: u64,
        _: types::Advice,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn sync_data(
        &mut self,
        _: Resource<types::Descriptor>,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn get_flags(
        &mut self,
        _: Resource<types::Descriptor>,
    ) -> Result<Result<types::DescriptorFlags, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn get_type(
        &mut self,
        _: Resource<types::Descriptor>,
    ) -> Result<Result<types::DescriptorType, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn set_size(
        &mut self,
        _: Resource<types::Descriptor>,
        _: u64,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn set_times(
        &mut self,
        _: Resource<types::Descriptor>,
        _: types::NewTimestamp,
        _: types::NewTimestamp,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn read(
        &mut self,
        _: Resource<types::Descriptor>,
        _: u64,
        _: u64,
    ) -> Result<Result<(Vec<u8>, bool), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn write(
        &mut self,
        _: Resource<types::Descriptor>,
        _: Vec<u8>,
        _: u64,
    ) -> Result<Result<u64, types::ErrorCode>> {
        unreachable!("no filesystem")
    }

    fn read_directory(
        &mut self,
        _: Resource<types::Descriptor>,
    ) -> Result<Result<Resource<types::DirectoryEntryStream>, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn sync(&mut self, _: Resource<types::Descriptor>) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn create_directory_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: String,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn stat(
        &mut self,
        _: Resource<types::Descriptor>,
    ) -> Result<Result<types::DescriptorStat, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn stat_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: types::PathFlags,
        _: String,
    ) -> Result<Result<types::DescriptorStat, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn set_times_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: types::PathFlags,
        _: String,
        _: types::NewTimestamp,
        _: types::NewTimestamp,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn link_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: types::PathFlags,
        _: String,
        _: Resource<types::Descriptor>,
        _: String,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn open_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: types::PathFlags,
        _: String,
        _: types::OpenFlags,
        _: types::DescriptorFlags,
    ) -> Result<Result<Resource<types::Descriptor>, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn readlink_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: String,
    ) -> Result<Result<String, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn remove_directory_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: String,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn rename_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: String,
        _: Resource<types::Descriptor>,
        _: String,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn symlink_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: String,
        _: String,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn unlink_file_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: String,
    ) -> Result<Result<(), types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn is_same_object(
        &mut self,
        _: Resource<types::Descriptor>,
        _: Resource<types::Descriptor>,
    ) -> Result<bool> {
        unreachable!("no filesystem")
    }
    fn metadata_hash(
        &mut self,
        _: Resource<types::Descriptor>,
    ) -> Result<Result<types::MetadataHashValue, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn metadata_hash_at(
        &mut self,
        _: Resource<types::Descriptor>,
        _: types::PathFlags,
        _: String,
    ) -> Result<Result<types::MetadataHashValue, types::ErrorCode>> {
        unreachable!("no filesystem")
    }

    fn drop(&mut self, _: Resource<types::Descriptor>) -> Result<()> {
        unreachable!("no filesystem")
    }
}
impl types::HostDirectoryEntryStream for WorkflowCtx {
    fn read_directory_entry(
        &mut self,
        _: Resource<types::DirectoryEntryStream>,
    ) -> Result<Result<Option<types::DirectoryEntry>, types::ErrorCode>> {
        unreachable!("no filesystem")
    }
    fn drop(&mut self, _: Resource<types::DirectoryEntryStream>) -> Result<()> {
        unreachable!("no filesystem")
    }
}
impl types::Host for WorkflowCtx {
    fn filesystem_error_code(
        &mut self,
        _: Resource<wasmtime_wasi_io::streams::Error>,
    ) -> Result<Option<types::ErrorCode>> {
        Ok(None)
    }
}
