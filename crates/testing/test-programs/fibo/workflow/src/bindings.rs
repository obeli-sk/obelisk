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
        }
        #[allow(dead_code, clippy::all)]
        pub mod execution {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type JoinSetId = _rt::String;
            pub type ExecutionId = _rt::String;
            #[derive(Clone)]
            pub enum ExecutionError {
                PermanentFailure(_rt::String),
                /// trap, instantiation error, non determinism, unhandled child execution error, param/result parsing error
                PermanentTimeout,
                NonDeterminism,
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
                        ExecutionError::NonDeterminism => {
                            f.debug_tuple("ExecutionError::NonDeterminism").finish()
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
            pub fn sleep(nanos: Duration) {
                unsafe {
                    use super::super::super::obelisk::types::time::Duration as V0;
                    let (result1_0, result1_1) = match nanos {
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
            pub fn new_join_set() -> JoinSetId {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:workflow/host-activities")]
                    extern "C" {
                        #[link_name = "new-join-set"]
                        fn wit_import(_: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0);
                    let l1 = *ptr0.add(0).cast::<*mut u8>();
                    let l2 = *ptr0.add(4).cast::<usize>();
                    let len3 = l2;
                    let bytes3 = _rt::Vec::from_raw_parts(l1.cast(), len3, len3);
                    _rt::string_lift(bytes3)
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
            pub type ExecutionError =
                super::super::super::obelisk::types::execution::ExecutionError;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fibo_submit(join_set_id: &str, n: u8) -> _rt::String {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-obelisk-ext/fibo")]
                    extern "C" {
                        #[link_name = "fibo-submit"]
                        fn wit_import(_: *mut u8, _: usize, _: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0, _rt::as_i32(&n), ptr1);
                    let l2 = *ptr1.add(0).cast::<*mut u8>();
                    let l3 = *ptr1.add(4).cast::<usize>();
                    let len4 = l3;
                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                    _rt::string_lift(bytes4)
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fibo_await_next(
                join_set_id: &str,
            ) -> Result<(ExecutionId, u64), (ExecutionId, ExecutionError)> {
                unsafe {
                    #[repr(align(8))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 32]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 32]);
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-obelisk-ext/fibo")]
                    extern "C" {
                        #[link_name = "fibo-await-next"]
                        fn wit_import(_: *mut u8, _: usize, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0, ptr1);
                    let l2 = i32::from(*ptr1.add(0).cast::<u8>());
                    match l2 {
                        0 => {
                            let e = {
                                let l3 = *ptr1.add(8).cast::<*mut u8>();
                                let l4 = *ptr1.add(12).cast::<usize>();
                                let len5 = l4;
                                let bytes5 = _rt::Vec::from_raw_parts(l3.cast(), len5, len5);
                                let l6 = *ptr1.add(16).cast::<i64>();
                                (_rt::string_lift(bytes5), l6 as u64)
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l7 = *ptr1.add(8).cast::<*mut u8>();
                                let l8 = *ptr1.add(12).cast::<usize>();
                                let len9 = l8;
                                let bytes9 = _rt::Vec::from_raw_parts(l7.cast(), len9, len9);
                                let l10 = i32::from(*ptr1.add(16).cast::<u8>());
                                use super::super::super::obelisk::types::execution::ExecutionError as V14;
                                let v14 = match l10 {
                                    0 => {
                                        let e14 = {
                                            let l11 = *ptr1.add(20).cast::<*mut u8>();
                                            let l12 = *ptr1.add(24).cast::<usize>();
                                            let len13 = l12;
                                            let bytes13 =
                                                _rt::Vec::from_raw_parts(l11.cast(), len13, len13);
                                            _rt::string_lift(bytes13)
                                        };
                                        V14::PermanentFailure(e14)
                                    }
                                    1 => V14::PermanentTimeout,
                                    n => {
                                        debug_assert_eq!(n, 2, "invalid enum discriminant");
                                        V14::NonDeterminism
                                    }
                                };
                                (_rt::string_lift(bytes9), v14)
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
            pub type ExecutionError =
                super::super::super::obelisk::types::execution::ExecutionError;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_submit(join_set_id: &str, n: u8, iterations: u32) -> _rt::String {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-submit"]
                        fn wit_import(_: *mut u8, _: usize, _: i32, _: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: i32, _: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(
                        ptr0.cast_mut(),
                        len0,
                        _rt::as_i32(&n),
                        _rt::as_i32(&iterations),
                        ptr1,
                    );
                    let l2 = *ptr1.add(0).cast::<*mut u8>();
                    let l3 = *ptr1.add(4).cast::<usize>();
                    let len4 = l3;
                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                    _rt::string_lift(bytes4)
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_await_next(
                join_set_id: &str,
            ) -> Result<(ExecutionId, u64), (ExecutionId, ExecutionError)> {
                unsafe {
                    #[repr(align(8))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 32]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 32]);
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-await-next"]
                        fn wit_import(_: *mut u8, _: usize, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0, ptr1);
                    let l2 = i32::from(*ptr1.add(0).cast::<u8>());
                    match l2 {
                        0 => {
                            let e = {
                                let l3 = *ptr1.add(8).cast::<*mut u8>();
                                let l4 = *ptr1.add(12).cast::<usize>();
                                let len5 = l4;
                                let bytes5 = _rt::Vec::from_raw_parts(l3.cast(), len5, len5);
                                let l6 = *ptr1.add(16).cast::<i64>();
                                (_rt::string_lift(bytes5), l6 as u64)
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l7 = *ptr1.add(8).cast::<*mut u8>();
                                let l8 = *ptr1.add(12).cast::<usize>();
                                let len9 = l8;
                                let bytes9 = _rt::Vec::from_raw_parts(l7.cast(), len9, len9);
                                let l10 = i32::from(*ptr1.add(16).cast::<u8>());
                                use super::super::super::obelisk::types::execution::ExecutionError as V14;
                                let v14 = match l10 {
                                    0 => {
                                        let e14 = {
                                            let l11 = *ptr1.add(20).cast::<*mut u8>();
                                            let l12 = *ptr1.add(24).cast::<usize>();
                                            let len13 = l12;
                                            let bytes13 =
                                                _rt::Vec::from_raw_parts(l11.cast(), len13, len13);
                                            _rt::string_lift(bytes13)
                                        };
                                        V14::PermanentFailure(e14)
                                    }
                                    1 => V14::PermanentTimeout,
                                    n => {
                                        debug_assert_eq!(n, 2, "invalid enum discriminant");
                                        V14::NonDeterminism
                                    }
                                };
                                (_rt::string_lift(bytes9), v14)
                            };
                            Err(e)
                        }
                        _ => _rt::invalid_enum_discriminant(),
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_concurrent_submit(
                join_set_id: &str,
                n: u8,
                iterations: u32,
            ) -> _rt::String {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-concurrent-submit"]
                        fn wit_import(_: *mut u8, _: usize, _: i32, _: i32, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: i32, _: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(
                        ptr0.cast_mut(),
                        len0,
                        _rt::as_i32(&n),
                        _rt::as_i32(&iterations),
                        ptr1,
                    );
                    let l2 = *ptr1.add(0).cast::<*mut u8>();
                    let l3 = *ptr1.add(4).cast::<usize>();
                    let len4 = l3;
                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                    _rt::string_lift(bytes4)
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_concurrent_await_next(
                join_set_id: &str,
            ) -> Result<(ExecutionId, u64), (ExecutionId, ExecutionError)> {
                unsafe {
                    #[repr(align(8))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 32]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 32]);
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow")]
                    extern "C" {
                        #[link_name = "fiboa-concurrent-await-next"]
                        fn wit_import(_: *mut u8, _: usize, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0, ptr1);
                    let l2 = i32::from(*ptr1.add(0).cast::<u8>());
                    match l2 {
                        0 => {
                            let e = {
                                let l3 = *ptr1.add(8).cast::<*mut u8>();
                                let l4 = *ptr1.add(12).cast::<usize>();
                                let len5 = l4;
                                let bytes5 = _rt::Vec::from_raw_parts(l3.cast(), len5, len5);
                                let l6 = *ptr1.add(16).cast::<i64>();
                                (_rt::string_lift(bytes5), l6 as u64)
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l7 = *ptr1.add(8).cast::<*mut u8>();
                                let l8 = *ptr1.add(12).cast::<usize>();
                                let len9 = l8;
                                let bytes9 = _rt::Vec::from_raw_parts(l7.cast(), len9, len9);
                                let l10 = i32::from(*ptr1.add(16).cast::<u8>());
                                use super::super::super::obelisk::types::execution::ExecutionError as V14;
                                let v14 = match l10 {
                                    0 => {
                                        let e14 = {
                                            let l11 = *ptr1.add(20).cast::<*mut u8>();
                                            let l12 = *ptr1.add(24).cast::<usize>();
                                            let len13 = l12;
                                            let bytes13 =
                                                _rt::Vec::from_raw_parts(l11.cast(), len13, len13);
                                            _rt::string_lift(bytes13)
                                        };
                                        V14::PermanentFailure(e14)
                                    }
                                    1 => V14::PermanentTimeout,
                                    n => {
                                        debug_assert_eq!(n, 2, "invalid enum discriminant");
                                        V14::NonDeterminism
                                    }
                                };
                                (_rt::string_lift(bytes9), v14)
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
#[link_section = "component-type:wit-bindgen:0.30.0:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 1707] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07\xb1\x0c\x01A\x02\x01\
A\x1a\x01B\x06\x01@\x01\x07messages\x01\0\x04\0\x05trace\x01\0\x04\0\x05debug\x01\
\0\x04\0\x04info\x01\0\x04\0\x04warn\x01\0\x04\0\x05error\x01\0\x03\x01\x0fobeli\
sk:log/log\x05\0\x01B\x06\x01q\x05\x0cmilliseconds\x01w\0\x07seconds\x01w\0\x07m\
inutes\x01y\0\x05hours\x01y\0\x04days\x01y\0\x04\0\x08duration\x03\0\0\x01r\x02\x07\
secondsw\x0bnanosecondsy\x04\0\x08datetime\x03\0\x02\x01q\x03\x03now\0\0\x02at\x01\
\x03\0\x02in\x01\x01\0\x04\0\x0bschedule-at\x03\0\x04\x03\x01\x12obelisk:types/t\
ime\x05\x01\x01B\x06\x01s\x04\0\x0bjoin-set-id\x03\0\0\x01s\x04\0\x0cexecution-i\
d\x03\0\x02\x01q\x03\x11permanent-failure\x01s\0\x11permanent-timeout\0\0\x0fnon\
-determinism\0\0\x04\0\x0fexecution-error\x03\0\x04\x03\x01\x17obelisk:types/exe\
cution\x05\x02\x02\x03\0\x01\x08duration\x02\x03\0\x02\x0bjoin-set-id\x01B\x08\x02\
\x03\x02\x01\x03\x04\0\x08duration\x03\0\0\x02\x03\x02\x01\x04\x04\0\x0bjoin-set\
-id\x03\0\x02\x01@\x01\x05nanos\x01\x01\0\x04\0\x05sleep\x01\x04\x01@\0\0\x03\x04\
\0\x0cnew-join-set\x01\x05\x03\x01\x20obelisk:workflow/host-activities\x05\x05\x02\
\x03\0\x02\x0cexecution-id\x02\x03\0\x02\x0fexecution-error\x01B\x0b\x02\x03\x02\
\x01\x06\x04\0\x0cexecution-id\x03\0\0\x02\x03\x02\x01\x07\x04\0\x0fexecution-er\
ror\x03\0\x02\x01@\x02\x0bjoin-set-ids\x01n}\0s\x04\0\x0bfibo-submit\x01\x04\x01\
o\x02\x01w\x01o\x02\x01\x03\x01j\x01\x05\x01\x06\x01@\x01\x0bjoin-set-ids\0\x07\x04\
\0\x0ffibo-await-next\x01\x08\x03\x01\x1dtesting:fibo-obelisk-ext/fibo\x05\x08\x01\
B\x0d\x02\x03\x02\x01\x06\x04\0\x0cexecution-id\x03\0\0\x02\x03\x02\x01\x07\x04\0\
\x0fexecution-error\x03\0\x02\x01@\x03\x0bjoin-set-ids\x01n}\x0aiterationsy\0s\x04\
\0\x0cfiboa-submit\x01\x04\x01o\x02\x01w\x01o\x02\x01\x03\x01j\x01\x05\x01\x06\x01\
@\x01\x0bjoin-set-ids\0\x07\x04\0\x10fiboa-await-next\x01\x08\x04\0\x17fiboa-con\
current-submit\x01\x04\x04\0\x1bfiboa-concurrent-await-next\x01\x08\x03\x01*test\
ing:fibo-workflow-obelisk-ext/workflow\x05\x09\x01B\x04\x01@\x01\x01n}\0w\x04\0\x14\
fibo-nested-workflow\x01\0\x01@\x03\x01n}\x06fiboasy\x14iterations-per-fiboay\0w\
\x04\0\x11fibo-start-fiboas\x01\x01\x03\x01&testing:fibo-workflow/workflow-nesti\
ng\x05\x0a\x01B\x04\x01@\x02\x01n}\x0aiterationsy\0w\x04\0\x05fibow\x01\0\x04\0\x05\
fiboa\x01\0\x04\0\x10fiboa-concurrent\x01\0\x03\x01\x1etesting:fibo-workflow/wor\
kflow\x05\x0b\x01B\x02\x01@\x01\x01n}\0w\x04\0\x04fibo\x01\0\x03\x01\x11testing:\
fibo/fibo\x05\x0c\x01B\x04\x01@\x01\x01n}\0w\x04\0\x14fibo-nested-workflow\x01\0\
\x01@\x03\x01n}\x06fiboasy\x14iterations-per-fiboay\0w\x04\0\x11fibo-start-fiboa\
s\x01\x01\x04\x01&testing:fibo-workflow/workflow-nesting\x05\x0d\x01B\x04\x01@\x02\
\x01n}\x0aiterationsy\0w\x04\0\x05fibow\x01\0\x04\0\x05fiboa\x01\0\x04\0\x10fibo\
a-concurrent\x01\0\x04\x01\x1etesting:fibo-workflow/workflow\x05\x0e\x04\x01\x0b\
any:any/any\x04\0\x0b\x09\x01\0\x03any\x03\0\0\0G\x09producers\x01\x0cprocessed-\
by\x02\x0dwit-component\x070.215.0\x10wit-bindgen-rust\x060.30.0";
#[inline(never)]
#[doc(hidden)]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
