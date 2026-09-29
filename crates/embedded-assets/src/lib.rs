#[cfg(any(feature = "embed-webui", feature = "embed-js-runtimes"))]
include!(concat!(env!("OUT_DIR"), "/gen.rs"));

pub const ACTIVITY_JS_RUNTIME_LOCATION: &str = include_str!("../activity-js-runtime-version.txt");
pub const ACTIVITY_VM_BOCHS_WASM_RUNTIME_LOCATION: &str =
    include_str!("../activity-vm-bochs-wasm-runtime-version.txt");
pub const ACTIVITY_VM_QEMU_TCG_RUNTIME_LOCATION: &str =
    include_str!("../activity-vm-qemu-tcg-runtime-version.txt");
pub const ACTIVITY_VM_QEMU_KVM_RUNTIME_LOCATION: &str =
    include_str!("../activity-vm-qemu-kvm-runtime-version.txt");
pub const ACTIVITY_VM_FIRECRACKER_RUNTIME_LOCATION: &str =
    include_str!("../activity-vm-firecracker-runtime-version.txt");
pub const WORKFLOW_JS_RUNTIME_LOCATION: &str = include_str!("../workflow-js-runtime-version.txt");
pub const WEBHOOK_JS_RUNTIME_LOCATION: &str = include_str!("../webhook-js-runtime-version.txt");
pub const WEBUI_LOCATION: &str = include_str!("../webui-version.txt");
