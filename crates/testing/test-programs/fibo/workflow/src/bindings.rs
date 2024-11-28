#[allow(dead_code)]
pub mod obelisk {
    #[allow(dead_code)]
    pub mod log {
        #[allow(dead_code, clippy::all)]
        pub mod log {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            #[allow(unused_unsafe, clippy::all)]
            pub fn trace(message: &str) {
                unsafe {
                    let vec0 = message;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:log/log")]
                    extern "C" {
                        #[link_name = "trace"]
                        fn wit_import(_: *mut u8, _: usize);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0);
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn debug(message: &str) {
                unsafe {
                    let vec0 = message;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:log/log")]
                    extern "C" {
                        #[link_name = "debug"]
                        fn wit_import(_: *mut u8, _: usize);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0);
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn info(message: &str) {
                unsafe {
                    let vec0 = message;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:log/log")]
                    extern "C" {
                        #[link_name = "info"]
                        fn wit_import(_: *mut u8, _: usize);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0);
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn warn(message: &str) {
                unsafe {
                    let vec0 = message;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:log/log")]
                    extern "C" {
                        #[link_name = "warn"]
                        fn wit_import(_: *mut u8, _: usize);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0);
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn error(message: &str) {
                unsafe {
                    let vec0 = message;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:log/log")]
                    extern "C" {
                        #[link_name = "error"]
                        fn wit_import(_: *mut u8, _: usize);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0);
                }
            }
        }
    }
    #[allow(dead_code)]
    pub mod types {
        #[allow(dead_code, clippy::all)]
        pub mod time {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            #[derive(Clone, Copy)]
            pub enum Duration {
                Milliseconds(u64),
                Seconds(u64),
                Minutes(u32),
                Hours(u32),
                Days(u32),
            }
            impl ::core::fmt::Debug for Duration {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    match self {
                        Duration::Milliseconds(e) => {
                            f.debug_tuple("Duration::Milliseconds").field(e).finish()
                        }
                        Duration::Seconds(e) => {
                            f.debug_tuple("Duration::Seconds").field(e).finish()
                        }
                        Duration::Minutes(e) => {
                            f.debug_tuple("Duration::Minutes").field(e).finish()
                        }
                        Duration::Hours(e) => f.debug_tuple("Duration::Hours").field(e).finish(),
                        Duration::Days(e) => f.debug_tuple("Duration::Days").field(e).finish(),
                    }
                }
            }
            /// A time and date in seconds plus nanoseconds.
            /// Extracted from wasi:clocks@0.2.0 to avoid dependency on wasi:io
            #[repr(C)]
            #[derive(Clone, Copy)]
            pub struct Datetime {
                pub seconds: u64,
                pub nanoseconds: u32,
            }
            impl ::core::fmt::Debug for Datetime {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct("Datetime")
                        .field("seconds", &self.seconds)
                        .field("nanoseconds", &self.nanoseconds)
                        .finish()
                }
            }
            #[derive(Clone, Copy)]
            pub enum ScheduleAt {
                Now,
                At(Datetime),
                In(Duration),
            }
            impl ::core::fmt::Debug for ScheduleAt {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    match self {
                        ScheduleAt::Now => f.debug_tuple("ScheduleAt::Now").finish(),
                        ScheduleAt::At(e) => f.debug_tuple("ScheduleAt::At").field(e).finish(),
                        ScheduleAt::In(e) => f.debug_tuple("ScheduleAt::In").field(e).finish(),
                    }
                }
            }
        }
        #[allow(dead_code, clippy::all)]
        pub mod execution {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[derive(Debug)]
            #[repr(transparent)]
            pub struct JoinSetId {
                handle: _rt::Resource<JoinSetId>,
            }
            impl JoinSetId {
                #[doc(hidden)]
                pub unsafe fn from_handle(handle: u32) -> Self {
                    Self {
                        handle: _rt::Resource::from_handle(handle),
                    }
                }
                #[doc(hidden)]
                pub fn take_handle(&self) -> u32 {
                    _rt::Resource::take_handle(&self.handle)
                }
                #[doc(hidden)]
                pub fn handle(&self) -> u32 {
                    _rt::Resource::handle(&self.handle)
                }
            }
            unsafe impl _rt::WasmResource for JoinSetId {
                #[inline]
                unsafe fn drop(_handle: u32) {
                    #[cfg(not(target_arch = "wasm32"))]
                    unreachable!();
                    #[cfg(target_arch = "wasm32")]
                    {
                        #[link(wasm_import_module = "obelisk:types/execution")]
                        extern "C" {
                            #[link_name = "[resource-drop]join-set-id"]
                            fn drop(_: u32);
                        }
                        drop(_handle);
                    }
                }
            }
            #[derive(Clone)]
            pub struct ExecutionId {
                pub id: _rt::String,
            }
            impl ::core::fmt::Debug for ExecutionId {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct("ExecutionId").field("id", &self.id).finish()
                }
            }
            #[derive(Clone)]
            pub enum ExecutionError {
                PermanentFailure(_rt::String),
                /// trap, instantiation error, non determinism, unhandled child execution error, param/result parsing error
                PermanentTimeout,
                Nondeterminism,
            }
            impl ::core::fmt::Debug for ExecutionError {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    match self {
                        ExecutionError::PermanentFailure(e) => f
                            .debug_tuple("ExecutionError::PermanentFailure")
                            .field(e)
                            .finish(),
                        ExecutionError::PermanentTimeout => {
                            f.debug_tuple("ExecutionError::PermanentTimeout").finish()
                        }
                        ExecutionError::Nondeterminism => {
                            f.debug_tuple("ExecutionError::Nondeterminism").finish()
                        }
                    }
                }
            }
        }
    }
    #[allow(dead_code)]
    pub mod workflow {
        #[allow(dead_code, clippy::all)]
        pub mod host_activities {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type Duration = super::super::super::obelisk::types::time::Duration;
            pub type JoinSetId = super::super::super::obelisk::types::execution::JoinSetId;
            #[allow(unused_unsafe, clippy::all)]
            /// Persistent sleep.
            pub fn sleep(duration: Duration) {
                unsafe {
                    use super::super::super::obelisk::types::time::Duration as V0;
                    let (result1_0, result1_1) = match duration {
                        V0::Milliseconds(e) => (0i32, _rt::as_i64(e)),
                        V0::Seconds(e) => (1i32, _rt::as_i64(e)),
                        V0::Minutes(e) => (2i32, i64::from(_rt::as_i32(e))),
                        V0::Hours(e) => (3i32, i64::from(_rt::as_i32(e))),
                        V0::Days(e) => (4i32, i64::from(_rt::as_i32(e))),
                    };
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:workflow/host-activities")]
                    extern "C" {
                        #[link_name = "sleep"]
                        fn wit_import(_: i32, _: i64);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i64) {
                        unreachable!()
                    }
                    wit_import(result1_0, result1_1);
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            /// Create new join set. Closing the join set at the execution finish will block until all child executions are finished.
            pub fn new_join_set() -> JoinSetId {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:workflow/host-activities")]
                    extern "C" {
                        #[link_name = "new-join-set"]
                        fn wit_import() -> i32;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import() -> i32 {
                        unreachable!()
                    }
                    let ret = wit_import();
                    super::super::super::obelisk::types::execution::JoinSetId::from_handle(
                        ret as u32,
                    )
                }
            }
        }
    }
}
#[allow(dead_code)]
pub mod testing {
    #[allow(dead_code)]
    pub mod fibo {
        #[allow(dead_code, clippy::all)]
        pub mod fibo {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fibo(n: u8) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo/fibo")]
                    extern "C" {
                        #[link_name = "fibo"]
                        fn wit_import(_: i32) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(_rt::as_i32(&n));
                    ret as u64
                }
            }
        }
    }
    #[allow(dead_code)]
    pub mod fibo_obelisk_ext {
        #[allow(dead_code, clippy::all)]
        pub mod fibo {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type ExecutionId = super::super::super::obelisk::types::execution::ExecutionId;
            pub type JoinSetId = super::super::super::obelisk::types::execution::JoinSetId;
            pub type ExecutionError =
                super::super::super::obelisk::types::execution::ExecutionError;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fibo_submit(join_set_id: &JoinSetId, n: u8) -> ExecutionId {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-obelisk-ext/fibo")]
                    extern "C" {
                        #[link_name = "fibo-submit"]
                        fn wit_import(_: i32, _: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import((join_set_id).handle() as i32, _rt::as_i32(&n), ptr0);
                    let l1 = *ptr0.add(0).cast::<*mut u8>();
                    let l2 = *ptr0.add(4).cast::<usize>();
                    let len3 = l2;
                    let bytes3 = _rt::Vec::from_raw_parts(l1.cast(), len3, len3);
                    super::super::super::obelisk::types::execution::ExecutionId {
                        id: _rt::string_lift(bytes3),
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fibo_await_next(
                join_set_id: &JoinSetId,
            ) -> Result<(ExecutionId, u64), (ExecutionId, ExecutionError)> {
                unsafe {
                    #[repr(align(8))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 32]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 32]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-obelisk-ext/fibo")]
                    extern "C" {
                        #[link_name = "fibo-await-next"]
                        fn wit_import(_: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import((join_set_id).handle() as i32, ptr0);
                    let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                    match l1 {
                        0 => {
                            let e = {
                                let l2 = *ptr0.add(8).cast::<*mut u8>();
                                let l3 = *ptr0.add(12).cast::<usize>();
                                let len4 = l3;
                                let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                                let l5 = *ptr0.add(16).cast::<i64>();
                                (
                                    super::super::super::obelisk::types::execution::ExecutionId {
                                        id: _rt::string_lift(bytes4),
                                    },
                                    l5 as u64,
                                )
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l6 = *ptr0.add(8).cast::<*mut u8>();
                                let l7 = *ptr0.add(12).cast::<usize>();
                                let len8 = l7;
                                let bytes8 = _rt::Vec::from_raw_parts(l6.cast(), len8, len8);
                                let l9 = i32::from(*ptr0.add(16).cast::<u8>());
                                use super::super::super::obelisk::types::execution::ExecutionError as V13;
                                let v13 = match l9 {
                                    0 => {
                                        let e13 = {
                                            let l10 = *ptr0.add(20).cast::<*mut u8>();
                                            let l11 = *ptr0.add(24).cast::<usize>();
                                            let len12 = l11;
                                            let bytes12 =
                                                _rt::Vec::from_raw_parts(l10.cast(), len12, len12);
                                            _rt::string_lift(bytes12)
                                        };
                                        V13::PermanentFailure(e13)
                                    }
                                    1 => V13::PermanentTimeout,
                                    n => {
                                        debug_assert_eq!(n, 2, "invalid enum discriminant");
                                        V13::Nondeterminism
                                    }
                                };
                                (
                                    super::super::super::obelisk::types::execution::ExecutionId {
                                        id: _rt::string_lift(bytes8),
                                    },
                                    v13,
                                )
                            };
                            Err(e)
                        }
                        _ => _rt::invalid_enum_discriminant(),
                    }
                }
            }
        }
    }
    #[allow(dead_code)]
    pub mod fibo_workflow {
        #[allow(dead_code, clippy::all)]
        pub mod workflow_nesting {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fibo_nested_workflow(n: u8) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow/workflow-nesting")]
                    extern "C" {
                        #[link_name = "fibo-nested-workflow"]
                        fn wit_import(_: i32) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(_rt::as_i32(&n));
                    ret as u64
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fibo_start_fiboas(n: u8, fiboas: u32, iterations_per_fiboa: u32) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow/workflow-nesting")]
                    extern "C" {
                        #[link_name = "fibo-start-fiboas"]
                        fn wit_import(_: i32, _: i32, _: i32) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32, _: i32) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(
                        _rt::as_i32(&n),
                        _rt::as_i32(&fiboas),
                        _rt::as_i32(&iterations_per_fiboa),
                    );
                    ret as u64
                }
            }
        }
        #[allow(dead_code, clippy::all)]
        pub mod workflow {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fibow(n: u8, iterations: u32) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow/workflow")]
                    extern "C" {
                        #[link_name = "fibow"]
                        fn wit_import(_: i32, _: i32) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(_rt::as_i32(&n), _rt::as_i32(&iterations));
                    ret as u64
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa(n: u8, iterations: u32) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow/workflow")]
                    extern "C" {
                        #[link_name = "fiboa"]
                        fn wit_import(_: i32, _: i32) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(_rt::as_i32(&n), _rt::as_i32(&iterations));
                    ret as u64
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_concurrent(n: u8, iterations: u32) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-concurrent"]
                        fn wit_import(_: i32, _: i32) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(_rt::as_i32(&n), _rt::as_i32(&iterations));
                    ret as u64
                }
            }
        }
    }
    #[allow(dead_code)]
    pub mod fibo_workflow_obelisk_ext {
        #[allow(dead_code, clippy::all)]
        pub mod workflow {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type ExecutionId = super::super::super::obelisk::types::execution::ExecutionId;
            pub type JoinSetId = super::super::super::obelisk::types::execution::JoinSetId;
            pub type ExecutionError =
                super::super::super::obelisk::types::execution::ExecutionError;
            pub type ScheduleAt = super::super::super::obelisk::types::time::ScheduleAt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_submit(join_set_id: &JoinSetId, n: u8, iterations: u32) -> ExecutionId {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-submit"]
                        fn wit_import(_: i32, _: i32, _: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32, _: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(
                        (join_set_id).handle() as i32,
                        _rt::as_i32(&n),
                        _rt::as_i32(&iterations),
                        ptr0,
                    );
                    let l1 = *ptr0.add(0).cast::<*mut u8>();
                    let l2 = *ptr0.add(4).cast::<usize>();
                    let len3 = l2;
                    let bytes3 = _rt::Vec::from_raw_parts(l1.cast(), len3, len3);
                    super::super::super::obelisk::types::execution::ExecutionId {
                        id: _rt::string_lift(bytes3),
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_await_next(
                join_set_id: &JoinSetId,
            ) -> Result<(ExecutionId, u64), (ExecutionId, ExecutionError)> {
                unsafe {
                    #[repr(align(8))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 32]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 32]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-await-next"]
                        fn wit_import(_: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import((join_set_id).handle() as i32, ptr0);
                    let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                    match l1 {
                        0 => {
                            let e = {
                                let l2 = *ptr0.add(8).cast::<*mut u8>();
                                let l3 = *ptr0.add(12).cast::<usize>();
                                let len4 = l3;
                                let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                                let l5 = *ptr0.add(16).cast::<i64>();
                                (
                                    super::super::super::obelisk::types::execution::ExecutionId {
                                        id: _rt::string_lift(bytes4),
                                    },
                                    l5 as u64,
                                )
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l6 = *ptr0.add(8).cast::<*mut u8>();
                                let l7 = *ptr0.add(12).cast::<usize>();
                                let len8 = l7;
                                let bytes8 = _rt::Vec::from_raw_parts(l6.cast(), len8, len8);
                                let l9 = i32::from(*ptr0.add(16).cast::<u8>());
                                use super::super::super::obelisk::types::execution::ExecutionError as V13;
                                let v13 = match l9 {
                                    0 => {
                                        let e13 = {
                                            let l10 = *ptr0.add(20).cast::<*mut u8>();
                                            let l11 = *ptr0.add(24).cast::<usize>();
                                            let len12 = l11;
                                            let bytes12 =
                                                _rt::Vec::from_raw_parts(l10.cast(), len12, len12);
                                            _rt::string_lift(bytes12)
                                        };
                                        V13::PermanentFailure(e13)
                                    }
                                    1 => V13::PermanentTimeout,
                                    n => {
                                        debug_assert_eq!(n, 2, "invalid enum discriminant");
                                        V13::Nondeterminism
                                    }
                                };
                                (
                                    super::super::super::obelisk::types::execution::ExecutionId {
                                        id: _rt::string_lift(bytes8),
                                    },
                                    v13,
                                )
                            };
                            Err(e)
                        }
                        _ => _rt::invalid_enum_discriminant(),
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_schedule(schedule_at: ScheduleAt, n: u8, iterations: u32) -> ExecutionId {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    use super::super::super::obelisk::types::time::ScheduleAt as V3;
                    let (result4_0, result4_1, result4_2) = match schedule_at {
                        V3::Now => (0i32, 0i64, 0i64),
                        V3::At(e) => {
                            let super::super::super::obelisk::types::time::Datetime {
                                seconds: seconds0,
                                nanoseconds: nanoseconds0,
                            } = e;
                            (
                                1i32,
                                _rt::as_i64(seconds0),
                                i64::from(_rt::as_i32(nanoseconds0)),
                            )
                        }
                        V3::In(e) => {
                            use super::super::super::obelisk::types::time::Duration as V1;
                            let (result2_0, result2_1) = match e {
                                V1::Milliseconds(e) => (0i32, _rt::as_i64(e)),
                                V1::Seconds(e) => (1i32, _rt::as_i64(e)),
                                V1::Minutes(e) => (2i32, i64::from(_rt::as_i32(e))),
                                V1::Hours(e) => (3i32, i64::from(_rt::as_i32(e))),
                                V1::Days(e) => (4i32, i64::from(_rt::as_i32(e))),
                            };
                            (2i32, i64::from(result2_0), result2_1)
                        }
                    };
                    let ptr5 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-schedule"]
                        fn wit_import(_: i32, _: i64, _: i64, _: i32, _: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i64, _: i64, _: i32, _: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(
                        result4_0,
                        result4_1,
                        result4_2,
                        _rt::as_i32(&n),
                        _rt::as_i32(&iterations),
                        ptr5,
                    );
                    let l6 = *ptr5.add(0).cast::<*mut u8>();
                    let l7 = *ptr5.add(4).cast::<usize>();
                    let len8 = l7;
                    let bytes8 = _rt::Vec::from_raw_parts(l6.cast(), len8, len8);
                    super::super::super::obelisk::types::execution::ExecutionId {
                        id: _rt::string_lift(bytes8),
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_concurrent_submit(
                join_set_id: &JoinSetId,
                n: u8,
                iterations: u32,
            ) -> ExecutionId {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-concurrent-submit"]
                        fn wit_import(_: i32, _: i32, _: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32, _: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(
                        (join_set_id).handle() as i32,
                        _rt::as_i32(&n),
                        _rt::as_i32(&iterations),
                        ptr0,
                    );
                    let l1 = *ptr0.add(0).cast::<*mut u8>();
                    let l2 = *ptr0.add(4).cast::<usize>();
                    let len3 = l2;
                    let bytes3 = _rt::Vec::from_raw_parts(l1.cast(), len3, len3);
                    super::super::super::obelisk::types::execution::ExecutionId {
                        id: _rt::string_lift(bytes3),
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_concurrent_await_next(
                join_set_id: &JoinSetId,
            ) -> Result<(ExecutionId, u64), (ExecutionId, ExecutionError)> {
                unsafe {
                    #[repr(align(8))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 32]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 32]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-concurrent-await-next"]
                        fn wit_import(_: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import((join_set_id).handle() as i32, ptr0);
                    let l1 = i32::from(*ptr0.add(0).cast::<u8>());
                    match l1 {
                        0 => {
                            let e = {
                                let l2 = *ptr0.add(8).cast::<*mut u8>();
                                let l3 = *ptr0.add(12).cast::<usize>();
                                let len4 = l3;
                                let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                                let l5 = *ptr0.add(16).cast::<i64>();
                                (
                                    super::super::super::obelisk::types::execution::ExecutionId {
                                        id: _rt::string_lift(bytes4),
                                    },
                                    l5 as u64,
                                )
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l6 = *ptr0.add(8).cast::<*mut u8>();
                                let l7 = *ptr0.add(12).cast::<usize>();
                                let len8 = l7;
                                let bytes8 = _rt::Vec::from_raw_parts(l6.cast(), len8, len8);
                                let l9 = i32::from(*ptr0.add(16).cast::<u8>());
                                use super::super::super::obelisk::types::execution::ExecutionError as V13;
                                let v13 = match l9 {
                                    0 => {
                                        let e13 = {
                                            let l10 = *ptr0.add(20).cast::<*mut u8>();
                                            let l11 = *ptr0.add(24).cast::<usize>();
                                            let len12 = l11;
                                            let bytes12 =
                                                _rt::Vec::from_raw_parts(l10.cast(), len12, len12);
                                            _rt::string_lift(bytes12)
                                        };
                                        V13::PermanentFailure(e13)
                                    }
                                    1 => V13::PermanentTimeout,
                                    n => {
                                        debug_assert_eq!(n, 2, "invalid enum discriminant");
                                        V13::Nondeterminism
                                    }
                                };
                                (
                                    super::super::super::obelisk::types::execution::ExecutionId {
                                        id: _rt::string_lift(bytes8),
                                    },
                                    v13,
                                )
                            };
                            Err(e)
                        }
                        _ => _rt::invalid_enum_discriminant(),
                    }
                }
            }
        }
    }
}
#[allow(dead_code)]
pub mod exports {
    #[allow(dead_code)]
    pub mod testing {
        #[allow(dead_code)]
        pub mod fibo_workflow {
            #[allow(dead_code, clippy::all)]
            pub mod workflow_nesting {
                #[used]
                #[doc(hidden)]
                static __FORCE_SECTION_REF: fn() =
                    super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_fibo_nested_workflow_cabi<T: Guest>(arg0: i32) -> i64 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let result0 = T::fibo_nested_workflow(arg0 as u8);
                    _rt::as_i64(result0)
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_fibo_start_fiboas_cabi<T: Guest>(
                    arg0: i32,
                    arg1: i32,
                    arg2: i32,
                ) -> i64 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let result0 = T::fibo_start_fiboas(arg0 as u8, arg1 as u32, arg2 as u32);
                    _rt::as_i64(result0)
                }
                pub trait Guest {
                    fn fibo_nested_workflow(n: u8) -> u64;
                    fn fibo_start_fiboas(n: u8, fiboas: u32, iterations_per_fiboa: u32) -> u64;
                }
                #[doc(hidden)]
                macro_rules! __export_testing_fibo_workflow_workflow_nesting_cabi {
                    ($ty:ident with_types_in $($path_to_types:tt)*) => {
                        const _ : () = { #[export_name =
                        "testing:fibo-workflow/workflow-nesting#fibo-nested-workflow"]
                        unsafe extern "C" fn export_fibo_nested_workflow(arg0 : i32,) ->
                        i64 { $($path_to_types)*::
                        _export_fibo_nested_workflow_cabi::<$ty > (arg0) } #[export_name
                        = "testing:fibo-workflow/workflow-nesting#fibo-start-fiboas"]
                        unsafe extern "C" fn export_fibo_start_fiboas(arg0 : i32, arg1 :
                        i32, arg2 : i32,) -> i64 { $($path_to_types)*::
                        _export_fibo_start_fiboas_cabi::<$ty > (arg0, arg1, arg2) } };
                    };
                }
                #[doc(hidden)]
                pub(crate) use __export_testing_fibo_workflow_workflow_nesting_cabi;
            }
            #[allow(dead_code, clippy::all)]
            pub mod workflow {
                #[used]
                #[doc(hidden)]
                static __FORCE_SECTION_REF: fn() =
                    super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_fibow_cabi<T: Guest>(arg0: i32, arg1: i32) -> i64 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let result0 = T::fibow(arg0 as u8, arg1 as u32);
                    _rt::as_i64(result0)
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_fiboa_cabi<T: Guest>(arg0: i32, arg1: i32) -> i64 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let result0 = T::fiboa(arg0 as u8, arg1 as u32);
                    _rt::as_i64(result0)
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_fiboa_concurrent_cabi<T: Guest>(arg0: i32, arg1: i32) -> i64 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let result0 = T::fiboa_concurrent(arg0 as u8, arg1 as u32);
                    _rt::as_i64(result0)
                }
                pub trait Guest {
                    fn fibow(n: u8, iterations: u32) -> u64;
                    fn fiboa(n: u8, iterations: u32) -> u64;
                    fn fiboa_concurrent(n: u8, iterations: u32) -> u64;
                }
                #[doc(hidden)]
                macro_rules! __export_testing_fibo_workflow_workflow_cabi {
                    ($ty:ident with_types_in $($path_to_types:tt)*) => {
                        const _ : () = { #[export_name =
                        "testing:fibo-workflow/workflow#fibow"] unsafe extern "C" fn
                        export_fibow(arg0 : i32, arg1 : i32,) -> i64 {
                        $($path_to_types)*:: _export_fibow_cabi::<$ty > (arg0, arg1) }
                        #[export_name = "testing:fibo-workflow/workflow#fiboa"] unsafe
                        extern "C" fn export_fiboa(arg0 : i32, arg1 : i32,) -> i64 {
                        $($path_to_types)*:: _export_fiboa_cabi::<$ty > (arg0, arg1) }
                        #[export_name =
                        "testing:fibo-workflow/workflow#fiboa-concurrent"] unsafe extern
                        "C" fn export_fiboa_concurrent(arg0 : i32, arg1 : i32,) -> i64 {
                        $($path_to_types)*:: _export_fiboa_concurrent_cabi::<$ty > (arg0,
                        arg1) } };
                    };
                }
                #[doc(hidden)]
                pub(crate) use __export_testing_fibo_workflow_workflow_cabi;
            }
        }
    }
}
mod _rt {
    use core::fmt;
    use core::marker;
    use core::sync::atomic::{AtomicU32, Ordering::Relaxed};
    /// A type which represents a component model resource, either imported or
    /// exported into this component.
    ///
    /// This is a low-level wrapper which handles the lifetime of the resource
    /// (namely this has a destructor). The `T` provided defines the component model
    /// intrinsics that this wrapper uses.
    ///
    /// One of the chief purposes of this type is to provide `Deref` implementations
    /// to access the underlying data when it is owned.
    ///
    /// This type is primarily used in generated code for exported and imported
    /// resources.
    #[repr(transparent)]
    pub struct Resource<T: WasmResource> {
        handle: AtomicU32,
        _marker: marker::PhantomData<T>,
    }
    /// A trait which all wasm resources implement, namely providing the ability to
    /// drop a resource.
    ///
    /// This generally is implemented by generated code, not user-facing code.
    #[allow(clippy::missing_safety_doc)]
    pub unsafe trait WasmResource {
        /// Invokes the `[resource-drop]...` intrinsic.
        unsafe fn drop(handle: u32);
    }
    impl<T: WasmResource> Resource<T> {
        #[doc(hidden)]
        pub unsafe fn from_handle(handle: u32) -> Self {
            debug_assert!(handle != u32::MAX);
            Self {
                handle: AtomicU32::new(handle),
                _marker: marker::PhantomData,
            }
        }
        /// Takes ownership of the handle owned by `resource`.
        ///
        /// Note that this ideally would be `into_handle` taking `Resource<T>` by
        /// ownership. The code generator does not enable that in all situations,
        /// unfortunately, so this is provided instead.
        ///
        /// Also note that `take_handle` is in theory only ever called on values
        /// owned by a generated function. For example a generated function might
        /// take `Resource<T>` as an argument but then call `take_handle` on a
        /// reference to that argument. In that sense the dynamic nature of
        /// `take_handle` should only be exposed internally to generated code, not
        /// to user code.
        #[doc(hidden)]
        pub fn take_handle(resource: &Resource<T>) -> u32 {
            resource.handle.swap(u32::MAX, Relaxed)
        }
        #[doc(hidden)]
        pub fn handle(resource: &Resource<T>) -> u32 {
            resource.handle.load(Relaxed)
        }
    }
    impl<T: WasmResource> fmt::Debug for Resource<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Resource")
                .field("handle", &self.handle)
                .finish()
        }
    }
    impl<T: WasmResource> Drop for Resource<T> {
        fn drop(&mut self) {
            unsafe {
                match self.handle.load(Relaxed) {
                    u32::MAX => {}
                    other => T::drop(other),
                }
            }
        }
    }
    pub use alloc_crate::string::String;
    pub fn as_i64<T: AsI64>(t: T) -> i64 {
        t.as_i64()
    }
    pub trait AsI64 {
        fn as_i64(self) -> i64;
    }
    impl<'a, T: Copy + AsI64> AsI64 for &'a T {
        fn as_i64(self) -> i64 {
            (*self).as_i64()
        }
    }
    impl AsI64 for i64 {
        #[inline]
        fn as_i64(self) -> i64 {
            self as i64
        }
    }
    impl AsI64 for u64 {
        #[inline]
        fn as_i64(self) -> i64 {
            self as i64
        }
    }
    pub fn as_i32<T: AsI32>(t: T) -> i32 {
        t.as_i32()
    }
    pub trait AsI32 {
        fn as_i32(self) -> i32;
    }
    impl<'a, T: Copy + AsI32> AsI32 for &'a T {
        fn as_i32(self) -> i32 {
            (*self).as_i32()
        }
    }
    impl AsI32 for i32 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for u32 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for i16 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for u16 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for i8 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for u8 {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for char {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    impl AsI32 for usize {
        #[inline]
        fn as_i32(self) -> i32 {
            self as i32
        }
    }
    pub use alloc_crate::vec::Vec;
    pub unsafe fn string_lift(bytes: Vec<u8>) -> String {
        if cfg!(debug_assertions) {
            String::from_utf8(bytes).unwrap()
        } else {
            String::from_utf8_unchecked(bytes)
        }
    }
    pub unsafe fn invalid_enum_discriminant<T>() -> T {
        if cfg!(debug_assertions) {
            panic!("invalid enum discriminant")
        } else {
            core::hint::unreachable_unchecked()
        }
    }
    #[cfg(target_arch = "wasm32")]
    pub fn run_ctors_once() {
        wit_bindgen_rt::run_ctors_once();
    }
    extern crate alloc as alloc_crate;
}
/// Generates `#[no_mangle]` functions to export the specified type as the
/// root implementation of all generated traits.
///
/// For more information see the documentation of `wit_bindgen::generate!`.
///
/// ```rust
/// # macro_rules! export{ ($($t:tt)*) => (); }
/// # trait Guest {}
/// struct MyType;
///
/// impl Guest for MyType {
///     // ...
/// }
///
/// export!(MyType);
/// ```
#[allow(unused_macros)]
#[doc(hidden)]
macro_rules! __export_any_impl {
    ($ty:ident) => {
        self::export!($ty with_types_in self);
    };
    ($ty:ident with_types_in $($path_to_types_root:tt)*) => {
        $($path_to_types_root)*::
        exports::testing::fibo_workflow::workflow_nesting::__export_testing_fibo_workflow_workflow_nesting_cabi!($ty
        with_types_in $($path_to_types_root)*::
        exports::testing::fibo_workflow::workflow_nesting); $($path_to_types_root)*::
        exports::testing::fibo_workflow::workflow::__export_testing_fibo_workflow_workflow_cabi!($ty
        with_types_in $($path_to_types_root)*::
        exports::testing::fibo_workflow::workflow);
    };
}
#[doc(inline)]
pub(crate) use __export_any_impl as export;
#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:wit-bindgen:0.35.0:any:any:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 1875] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07\xd9\x0d\x01A\x02\x01\
A\x1b\x01B\x06\x01@\x01\x07messages\x01\0\x04\0\x05trace\x01\0\x04\0\x05debug\x01\
\0\x04\0\x04info\x01\0\x04\0\x04warn\x01\0\x04\0\x05error\x01\0\x03\0\x0fobelisk\
:log/log\x05\0\x01B\x06\x01q\x05\x0cmilliseconds\x01w\0\x07seconds\x01w\0\x07min\
utes\x01y\0\x05hours\x01y\0\x04days\x01y\0\x04\0\x08duration\x03\0\0\x01r\x02\x07\
secondsw\x0bnanosecondsy\x04\0\x08datetime\x03\0\x02\x01q\x03\x03now\0\0\x02at\x01\
\x03\0\x02in\x01\x01\0\x04\0\x0bschedule-at\x03\0\x04\x03\0\x12obelisk:types/tim\
e\x05\x01\x01B\x07\x04\0\x0bjoin-set-id\x03\x01\x01r\x01\x02ids\x04\0\x0cexecuti\
on-id\x03\0\x01\x01r\x01\x02ids\x04\0\x08delay-id\x03\0\x03\x01q\x03\x11permanen\
t-failure\x01s\0\x11permanent-timeout\0\0\x0enondeterminism\0\0\x04\0\x0fexecuti\
on-error\x03\0\x05\x03\0\x17obelisk:types/execution\x05\x02\x02\x03\0\x01\x08dur\
ation\x02\x03\0\x02\x0bjoin-set-id\x01B\x09\x02\x03\x02\x01\x03\x04\0\x08duratio\
n\x03\0\0\x02\x03\x02\x01\x04\x04\0\x0bjoin-set-id\x03\0\x02\x01@\x01\x08duratio\
n\x01\x01\0\x04\0\x05sleep\x01\x04\x01i\x03\x01@\0\0\x05\x04\0\x0cnew-join-set\x01\
\x06\x03\0\x20obelisk:workflow/host-activities\x05\x05\x02\x03\0\x02\x0cexecutio\
n-id\x02\x03\0\x02\x0fexecution-error\x01B\x0e\x02\x03\x02\x01\x06\x04\0\x0cexec\
ution-id\x03\0\0\x02\x03\x02\x01\x04\x04\0\x0bjoin-set-id\x03\0\x02\x02\x03\x02\x01\
\x07\x04\0\x0fexecution-error\x03\0\x04\x01h\x03\x01@\x02\x0bjoin-set-id\x06\x01\
n}\0\x01\x04\0\x0bfibo-submit\x01\x07\x01o\x02\x01w\x01o\x02\x01\x05\x01j\x01\x08\
\x01\x09\x01@\x01\x0bjoin-set-id\x06\0\x0a\x04\0\x0ffibo-await-next\x01\x0b\x03\0\
\x1dtesting:fibo-obelisk-ext/fibo\x05\x08\x02\x03\0\x01\x0bschedule-at\x01B\x14\x02\
\x03\x02\x01\x06\x04\0\x0cexecution-id\x03\0\0\x02\x03\x02\x01\x04\x04\0\x0bjoin\
-set-id\x03\0\x02\x02\x03\x02\x01\x07\x04\0\x0fexecution-error\x03\0\x04\x02\x03\
\x02\x01\x09\x04\0\x0bschedule-at\x03\0\x06\x01h\x03\x01@\x03\x0bjoin-set-id\x08\
\x01n}\x0aiterationsy\0\x01\x04\0\x0cfiboa-submit\x01\x09\x01o\x02\x01w\x01o\x02\
\x01\x05\x01j\x01\x0a\x01\x0b\x01@\x01\x0bjoin-set-id\x08\0\x0c\x04\0\x10fiboa-a\
wait-next\x01\x0d\x01@\x03\x0bschedule-at\x07\x01n}\x0aiterationsy\0\x01\x04\0\x0e\
fiboa-schedule\x01\x0e\x04\0\x17fiboa-concurrent-submit\x01\x09\x04\0\x1bfiboa-c\
oncurrent-await-next\x01\x0d\x03\0*testing:fibo-workflow-obelisk-ext/workflow\x05\
\x0a\x01B\x04\x01@\x01\x01n}\0w\x04\0\x14fibo-nested-workflow\x01\0\x01@\x03\x01\
n}\x06fiboasy\x14iterations-per-fiboay\0w\x04\0\x11fibo-start-fiboas\x01\x01\x03\
\0&testing:fibo-workflow/workflow-nesting\x05\x0b\x01B\x04\x01@\x02\x01n}\x0aite\
rationsy\0w\x04\0\x05fibow\x01\0\x04\0\x05fiboa\x01\0\x04\0\x10fiboa-concurrent\x01\
\0\x03\0\x1etesting:fibo-workflow/workflow\x05\x0c\x01B\x02\x01@\x01\x01n}\0w\x04\
\0\x04fibo\x01\0\x03\0\x11testing:fibo/fibo\x05\x0d\x01B\x04\x01@\x01\x01n}\0w\x04\
\0\x14fibo-nested-workflow\x01\0\x01@\x03\x01n}\x06fiboasy\x14iterations-per-fib\
oay\0w\x04\0\x11fibo-start-fiboas\x01\x01\x04\0&testing:fibo-workflow/workflow-n\
esting\x05\x0e\x01B\x04\x01@\x02\x01n}\x0aiterationsy\0w\x04\0\x05fibow\x01\0\x04\
\0\x05fiboa\x01\0\x04\0\x10fiboa-concurrent\x01\0\x04\0\x1etesting:fibo-workflow\
/workflow\x05\x0f\x04\0\x0bany:any/any\x04\0\x0b\x09\x01\0\x03any\x03\0\0\0G\x09\
producers\x01\x0cprocessed-by\x02\x0dwit-component\x070.220.0\x10wit-bindgen-rus\
t\x060.35.0";
#[inline(never)]
#[doc(hidden)]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
