#[allow(dead_code)]
pub mod obelisk {
    #[allow(dead_code)]
    pub mod types {
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
            impl JoinSetId {
                #[allow(unused_unsafe, clippy::all)]
                pub fn id(&self) -> _rt::String {
                    unsafe {
                        #[repr(align(4))]
                        struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                        let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                        let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                        #[cfg(target_arch = "wasm32")]
                        #[link(wasm_import_module = "obelisk:types/execution")]
                        extern "C" {
                            #[link_name = "[method]join-set-id.id"]
                            fn wit_import(_: i32, _: *mut u8);
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        fn wit_import(_: i32, _: *mut u8) {
                            unreachable!()
                        }
                        wit_import((self).handle() as i32, ptr0);
                        let l1 = *ptr0.add(0).cast::<*mut u8>();
                        let l2 = *ptr0.add(4).cast::<usize>();
                        let len3 = l2;
                        let bytes3 = _rt::Vec::from_raw_parts(l1.cast(), len3, len3);
                        _rt::string_lift(bytes3)
                    }
                }
            }
        }
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
    pub mod http {
        #[allow(dead_code, clippy::all)]
        pub mod http_get {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn get(url: &str) -> Result<_rt::String, _rt::String> {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                    let vec0 = url;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:http/http-get")]
                    extern "C" {
                        #[link_name = "get"]
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
                                let l3 = *ptr1.add(4).cast::<*mut u8>();
                                let l4 = *ptr1.add(8).cast::<usize>();
                                let len5 = l4;
                                let bytes5 = _rt::Vec::from_raw_parts(l3.cast(), len5, len5);
                                _rt::string_lift(bytes5)
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l6 = *ptr1.add(4).cast::<*mut u8>();
                                let l7 = *ptr1.add(8).cast::<usize>();
                                let len8 = l7;
                                let bytes8 = _rt::Vec::from_raw_parts(l6.cast(), len8, len8);
                                _rt::string_lift(bytes8)
                            };
                            Err(e)
                        }
                        _ => _rt::invalid_enum_discriminant(),
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn get_successful(url: &str) -> Result<_rt::String, _rt::String> {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                    let vec0 = url;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:http/http-get")]
                    extern "C" {
                        #[link_name = "get-successful"]
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
                                let l3 = *ptr1.add(4).cast::<*mut u8>();
                                let l4 = *ptr1.add(8).cast::<usize>();
                                let len5 = l4;
                                let bytes5 = _rt::Vec::from_raw_parts(l3.cast(), len5, len5);
                                _rt::string_lift(bytes5)
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l6 = *ptr1.add(4).cast::<*mut u8>();
                                let l7 = *ptr1.add(8).cast::<usize>();
                                let len8 = l7;
                                let bytes8 = _rt::Vec::from_raw_parts(l6.cast(), len8, len8);
                                _rt::string_lift(bytes8)
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
    pub mod http_obelisk_ext {
        #[allow(dead_code, clippy::all)]
        pub mod http_get {
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
            pub fn get_successful_submit(join_set_id: &JoinSetId, url: &str) -> ExecutionId {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let vec0 = url;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:http-obelisk-ext/http-get")]
                    extern "C" {
                        #[link_name = "get-successful-submit"]
                        fn wit_import(_: i32, _: *mut u8, _: usize, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: *mut u8, _: usize, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import((join_set_id).handle() as i32, ptr0.cast_mut(), len0, ptr1);
                    let l2 = *ptr1.add(0).cast::<*mut u8>();
                    let l3 = *ptr1.add(4).cast::<usize>();
                    let len4 = l3;
                    let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                    super::super::super::obelisk::types::execution::ExecutionId {
                        id: _rt::string_lift(bytes4),
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn get_successful_await_next(
                join_set_id: &JoinSetId,
            ) -> Result<
                (ExecutionId, Result<_rt::String, _rt::String>),
                (ExecutionId, ExecutionError),
            > {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 24]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 24]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:http-obelisk-ext/http-get")]
                    extern "C" {
                        #[link_name = "get-successful-await-next"]
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
                                let l2 = *ptr0.add(4).cast::<*mut u8>();
                                let l3 = *ptr0.add(8).cast::<usize>();
                                let len4 = l3;
                                let bytes4 = _rt::Vec::from_raw_parts(l2.cast(), len4, len4);
                                let l5 = i32::from(*ptr0.add(12).cast::<u8>());
                                (
                                    super::super::super::obelisk::types::execution::ExecutionId {
                                        id: _rt::string_lift(bytes4),
                                    },
                                    match l5 {
                                        0 => {
                                            let e = {
                                                let l6 = *ptr0.add(16).cast::<*mut u8>();
                                                let l7 = *ptr0.add(20).cast::<usize>();
                                                let len8 = l7;
                                                let bytes8 =
                                                    _rt::Vec::from_raw_parts(l6.cast(), len8, len8);
                                                _rt::string_lift(bytes8)
                                            };
                                            Ok(e)
                                        }
                                        1 => {
                                            let e = {
                                                let l9 = *ptr0.add(16).cast::<*mut u8>();
                                                let l10 = *ptr0.add(20).cast::<usize>();
                                                let len11 = l10;
                                                let bytes11 = _rt::Vec::from_raw_parts(
                                                    l9.cast(),
                                                    len11,
                                                    len11,
                                                );
                                                _rt::string_lift(bytes11)
                                            };
                                            Err(e)
                                        }
                                        _ => _rt::invalid_enum_discriminant(),
                                    },
                                )
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l12 = *ptr0.add(4).cast::<*mut u8>();
                                let l13 = *ptr0.add(8).cast::<usize>();
                                let len14 = l13;
                                let bytes14 = _rt::Vec::from_raw_parts(l12.cast(), len14, len14);
                                let l15 = i32::from(*ptr0.add(12).cast::<u8>());
                                use super::super::super::obelisk::types::execution::ExecutionError as V19;
                                let v19 = match l15 {
                                    0 => {
                                        let e19 = {
                                            let l16 = *ptr0.add(16).cast::<*mut u8>();
                                            let l17 = *ptr0.add(20).cast::<usize>();
                                            let len18 = l17;
                                            let bytes18 =
                                                _rt::Vec::from_raw_parts(l16.cast(), len18, len18);
                                            _rt::string_lift(bytes18)
                                        };
                                        V19::PermanentFailure(e19)
                                    }
                                    1 => V19::PermanentTimeout,
                                    n => {
                                        debug_assert_eq!(n, 2, "invalid enum discriminant");
                                        V19::Nondeterminism
                                    }
                                };
                                (
                                    super::super::super::obelisk::types::execution::ExecutionId {
                                        id: _rt::string_lift(bytes14),
                                    },
                                    v19,
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
        pub mod http_workflow {
            #[allow(dead_code, clippy::all)]
            pub mod workflow {
                #[used]
                #[doc(hidden)]
                static __FORCE_SECTION_REF: fn() =
                    super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_get_cabi<T: Guest>(arg0: *mut u8, arg1: usize) -> *mut u8 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let len0 = arg1;
                    let bytes0 = _rt::Vec::from_raw_parts(arg0.cast(), len0, len0);
                    let result1 = T::get(_rt::string_lift(bytes0));
                    let ptr2 = _RET_AREA.0.as_mut_ptr().cast::<u8>();
                    match result1 {
                        Ok(e) => {
                            *ptr2.add(0).cast::<u8>() = (0i32) as u8;
                            let vec3 = (e.into_bytes()).into_boxed_slice();
                            let ptr3 = vec3.as_ptr().cast::<u8>();
                            let len3 = vec3.len();
                            ::core::mem::forget(vec3);
                            *ptr2.add(8).cast::<usize>() = len3;
                            *ptr2.add(4).cast::<*mut u8>() = ptr3.cast_mut();
                        }
                        Err(e) => {
                            *ptr2.add(0).cast::<u8>() = (1i32) as u8;
                            let vec4 = (e.into_bytes()).into_boxed_slice();
                            let ptr4 = vec4.as_ptr().cast::<u8>();
                            let len4 = vec4.len();
                            ::core::mem::forget(vec4);
                            *ptr2.add(8).cast::<usize>() = len4;
                            *ptr2.add(4).cast::<*mut u8>() = ptr4.cast_mut();
                        }
                    };
                    ptr2
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn __post_return_get<T: Guest>(arg0: *mut u8) {
                    let l0 = i32::from(*arg0.add(0).cast::<u8>());
                    match l0 {
                        0 => {
                            let l1 = *arg0.add(4).cast::<*mut u8>();
                            let l2 = *arg0.add(8).cast::<usize>();
                            _rt::cabi_dealloc(l1, l2, 1);
                        }
                        _ => {
                            let l3 = *arg0.add(4).cast::<*mut u8>();
                            let l4 = *arg0.add(8).cast::<usize>();
                            _rt::cabi_dealloc(l3, l4, 1);
                        }
                    }
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_get_successful_cabi<T: Guest>(
                    arg0: *mut u8,
                    arg1: usize,
                ) -> *mut u8 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let len0 = arg1;
                    let bytes0 = _rt::Vec::from_raw_parts(arg0.cast(), len0, len0);
                    let result1 = T::get_successful(_rt::string_lift(bytes0));
                    let ptr2 = _RET_AREA.0.as_mut_ptr().cast::<u8>();
                    match result1 {
                        Ok(e) => {
                            *ptr2.add(0).cast::<u8>() = (0i32) as u8;
                            let vec3 = (e.into_bytes()).into_boxed_slice();
                            let ptr3 = vec3.as_ptr().cast::<u8>();
                            let len3 = vec3.len();
                            ::core::mem::forget(vec3);
                            *ptr2.add(8).cast::<usize>() = len3;
                            *ptr2.add(4).cast::<*mut u8>() = ptr3.cast_mut();
                        }
                        Err(e) => {
                            *ptr2.add(0).cast::<u8>() = (1i32) as u8;
                            let vec4 = (e.into_bytes()).into_boxed_slice();
                            let ptr4 = vec4.as_ptr().cast::<u8>();
                            let len4 = vec4.len();
                            ::core::mem::forget(vec4);
                            *ptr2.add(8).cast::<usize>() = len4;
                            *ptr2.add(4).cast::<*mut u8>() = ptr4.cast_mut();
                        }
                    };
                    ptr2
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn __post_return_get_successful<T: Guest>(arg0: *mut u8) {
                    let l0 = i32::from(*arg0.add(0).cast::<u8>());
                    match l0 {
                        0 => {
                            let l1 = *arg0.add(4).cast::<*mut u8>();
                            let l2 = *arg0.add(8).cast::<usize>();
                            _rt::cabi_dealloc(l1, l2, 1);
                        }
                        _ => {
                            let l3 = *arg0.add(4).cast::<*mut u8>();
                            let l4 = *arg0.add(8).cast::<usize>();
                            _rt::cabi_dealloc(l3, l4, 1);
                        }
                    }
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_get_successful_concurrently_cabi<T: Guest>(
                    arg0: *mut u8,
                    arg1: usize,
                ) -> *mut u8 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let base3 = arg0;
                    let len3 = arg1;
                    let mut result3 = _rt::Vec::with_capacity(len3);
                    for i in 0..len3 {
                        let base = base3.add(i * 8);
                        let e3 = {
                            let l0 = *base.add(0).cast::<*mut u8>();
                            let l1 = *base.add(4).cast::<usize>();
                            let len2 = l1;
                            let bytes2 = _rt::Vec::from_raw_parts(l0.cast(), len2, len2);
                            _rt::string_lift(bytes2)
                        };
                        result3.push(e3);
                    }
                    _rt::cabi_dealloc(base3, len3 * 8, 4);
                    let result4 = T::get_successful_concurrently(result3);
                    let ptr5 = _RET_AREA.0.as_mut_ptr().cast::<u8>();
                    match result4 {
                        Ok(e) => {
                            *ptr5.add(0).cast::<u8>() = (0i32) as u8;
                            let vec7 = e;
                            let len7 = vec7.len();
                            let layout7 =
                                _rt::alloc::Layout::from_size_align_unchecked(vec7.len() * 8, 4);
                            let result7 = if layout7.size() != 0 {
                                let ptr = _rt::alloc::alloc(layout7).cast::<u8>();
                                if ptr.is_null() {
                                    _rt::alloc::handle_alloc_error(layout7);
                                }
                                ptr
                            } else {
                                ::core::ptr::null_mut()
                            };
                            for (i, e) in vec7.into_iter().enumerate() {
                                let base = result7.add(i * 8);
                                {
                                    let vec6 = (e.into_bytes()).into_boxed_slice();
                                    let ptr6 = vec6.as_ptr().cast::<u8>();
                                    let len6 = vec6.len();
                                    ::core::mem::forget(vec6);
                                    *base.add(4).cast::<usize>() = len6;
                                    *base.add(0).cast::<*mut u8>() = ptr6.cast_mut();
                                }
                            }
                            *ptr5.add(8).cast::<usize>() = len7;
                            *ptr5.add(4).cast::<*mut u8>() = result7;
                        }
                        Err(e) => {
                            *ptr5.add(0).cast::<u8>() = (1i32) as u8;
                            let vec8 = (e.into_bytes()).into_boxed_slice();
                            let ptr8 = vec8.as_ptr().cast::<u8>();
                            let len8 = vec8.len();
                            ::core::mem::forget(vec8);
                            *ptr5.add(8).cast::<usize>() = len8;
                            *ptr5.add(4).cast::<*mut u8>() = ptr8.cast_mut();
                        }
                    };
                    ptr5
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn __post_return_get_successful_concurrently<T: Guest>(arg0: *mut u8) {
                    let l0 = i32::from(*arg0.add(0).cast::<u8>());
                    match l0 {
                        0 => {
                            let l1 = *arg0.add(4).cast::<*mut u8>();
                            let l2 = *arg0.add(8).cast::<usize>();
                            let base5 = l1;
                            let len5 = l2;
                            for i in 0..len5 {
                                let base = base5.add(i * 8);
                                {
                                    let l3 = *base.add(0).cast::<*mut u8>();
                                    let l4 = *base.add(4).cast::<usize>();
                                    _rt::cabi_dealloc(l3, l4, 1);
                                }
                            }
                            _rt::cabi_dealloc(base5, len5 * 8, 4);
                        }
                        _ => {
                            let l6 = *arg0.add(4).cast::<*mut u8>();
                            let l7 = *arg0.add(8).cast::<usize>();
                            _rt::cabi_dealloc(l6, l7, 1);
                        }
                    }
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_get_successful_concurrently_stress_cabi<T: Guest>(
                    arg0: *mut u8,
                    arg1: usize,
                    arg2: i32,
                ) -> *mut u8 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let len0 = arg1;
                    let bytes0 = _rt::Vec::from_raw_parts(arg0.cast(), len0, len0);
                    let result1 = T::get_successful_concurrently_stress(
                        _rt::string_lift(bytes0),
                        arg2 as u32,
                    );
                    let ptr2 = _RET_AREA.0.as_mut_ptr().cast::<u8>();
                    match result1 {
                        Ok(e) => {
                            *ptr2.add(0).cast::<u8>() = (0i32) as u8;
                            let vec4 = e;
                            let len4 = vec4.len();
                            let layout4 =
                                _rt::alloc::Layout::from_size_align_unchecked(vec4.len() * 8, 4);
                            let result4 = if layout4.size() != 0 {
                                let ptr = _rt::alloc::alloc(layout4).cast::<u8>();
                                if ptr.is_null() {
                                    _rt::alloc::handle_alloc_error(layout4);
                                }
                                ptr
                            } else {
                                ::core::ptr::null_mut()
                            };
                            for (i, e) in vec4.into_iter().enumerate() {
                                let base = result4.add(i * 8);
                                {
                                    let vec3 = (e.into_bytes()).into_boxed_slice();
                                    let ptr3 = vec3.as_ptr().cast::<u8>();
                                    let len3 = vec3.len();
                                    ::core::mem::forget(vec3);
                                    *base.add(4).cast::<usize>() = len3;
                                    *base.add(0).cast::<*mut u8>() = ptr3.cast_mut();
                                }
                            }
                            *ptr2.add(8).cast::<usize>() = len4;
                            *ptr2.add(4).cast::<*mut u8>() = result4;
                        }
                        Err(e) => {
                            *ptr2.add(0).cast::<u8>() = (1i32) as u8;
                            let vec5 = (e.into_bytes()).into_boxed_slice();
                            let ptr5 = vec5.as_ptr().cast::<u8>();
                            let len5 = vec5.len();
                            ::core::mem::forget(vec5);
                            *ptr2.add(8).cast::<usize>() = len5;
                            *ptr2.add(4).cast::<*mut u8>() = ptr5.cast_mut();
                        }
                    };
                    ptr2
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn __post_return_get_successful_concurrently_stress<T: Guest>(
                    arg0: *mut u8,
                ) {
                    let l0 = i32::from(*arg0.add(0).cast::<u8>());
                    match l0 {
                        0 => {
                            let l1 = *arg0.add(4).cast::<*mut u8>();
                            let l2 = *arg0.add(8).cast::<usize>();
                            let base5 = l1;
                            let len5 = l2;
                            for i in 0..len5 {
                                let base = base5.add(i * 8);
                                {
                                    let l3 = *base.add(0).cast::<*mut u8>();
                                    let l4 = *base.add(4).cast::<usize>();
                                    _rt::cabi_dealloc(l3, l4, 1);
                                }
                            }
                            _rt::cabi_dealloc(base5, len5 * 8, 4);
                        }
                        _ => {
                            let l6 = *arg0.add(4).cast::<*mut u8>();
                            let l7 = *arg0.add(8).cast::<usize>();
                            _rt::cabi_dealloc(l6, l7, 1);
                        }
                    }
                }
                pub trait Guest {
                    fn get(url: _rt::String) -> Result<_rt::String, _rt::String>;
                    fn get_successful(url: _rt::String) -> Result<_rt::String, _rt::String>;
                    fn get_successful_concurrently(
                        urls: _rt::Vec<_rt::String>,
                    ) -> Result<_rt::Vec<_rt::String>, _rt::String>;
                    fn get_successful_concurrently_stress(
                        url: _rt::String,
                        concurrency: u32,
                    ) -> Result<_rt::Vec<_rt::String>, _rt::String>;
                }
                #[doc(hidden)]
                macro_rules! __export_testing_http_workflow_workflow_cabi {
                    ($ty:ident with_types_in $($path_to_types:tt)*) => {
                        const _ : () = { #[export_name =
                        "testing:http-workflow/workflow#get"] unsafe extern "C" fn
                        export_get(arg0 : * mut u8, arg1 : usize,) -> * mut u8 {
                        $($path_to_types)*:: _export_get_cabi::<$ty > (arg0, arg1) }
                        #[export_name = "cabi_post_testing:http-workflow/workflow#get"]
                        unsafe extern "C" fn _post_return_get(arg0 : * mut u8,) {
                        $($path_to_types)*:: __post_return_get::<$ty > (arg0) }
                        #[export_name = "testing:http-workflow/workflow#get-successful"]
                        unsafe extern "C" fn export_get_successful(arg0 : * mut u8, arg1
                        : usize,) -> * mut u8 { $($path_to_types)*::
                        _export_get_successful_cabi::<$ty > (arg0, arg1) } #[export_name
                        = "cabi_post_testing:http-workflow/workflow#get-successful"]
                        unsafe extern "C" fn _post_return_get_successful(arg0 : * mut
                        u8,) { $($path_to_types)*:: __post_return_get_successful::<$ty >
                        (arg0) } #[export_name =
                        "testing:http-workflow/workflow#get-successful-concurrently"]
                        unsafe extern "C" fn export_get_successful_concurrently(arg0 : *
                        mut u8, arg1 : usize,) -> * mut u8 { $($path_to_types)*::
                        _export_get_successful_concurrently_cabi::<$ty > (arg0, arg1) }
                        #[export_name =
                        "cabi_post_testing:http-workflow/workflow#get-successful-concurrently"]
                        unsafe extern "C" fn
                        _post_return_get_successful_concurrently(arg0 : * mut u8,) {
                        $($path_to_types)*::
                        __post_return_get_successful_concurrently::<$ty > (arg0) }
                        #[export_name =
                        "testing:http-workflow/workflow#get-successful-concurrently-stress"]
                        unsafe extern "C" fn
                        export_get_successful_concurrently_stress(arg0 : * mut u8, arg1 :
                        usize, arg2 : i32,) -> * mut u8 { $($path_to_types)*::
                        _export_get_successful_concurrently_stress_cabi::<$ty > (arg0,
                        arg1, arg2) } #[export_name =
                        "cabi_post_testing:http-workflow/workflow#get-successful-concurrently-stress"]
                        unsafe extern "C" fn
                        _post_return_get_successful_concurrently_stress(arg0 : * mut u8,)
                        { $($path_to_types)*::
                        __post_return_get_successful_concurrently_stress::<$ty > (arg0) }
                        };
                    };
                }
                #[doc(hidden)]
                pub(crate) use __export_testing_http_workflow_workflow_cabi;
                #[repr(align(4))]
                struct _RetArea([::core::mem::MaybeUninit<u8>; 12]);
                static mut _RET_AREA: _RetArea = _RetArea([::core::mem::MaybeUninit::uninit(); 12]);
            }
        }
    }
}
mod _rt {
    pub use alloc_crate::string::String;
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
    #[cfg(target_arch = "wasm32")]
    pub fn run_ctors_once() {
        wit_bindgen_rt::run_ctors_once();
    }
    pub unsafe fn cabi_dealloc(ptr: *mut u8, size: usize, align: usize) {
        if size == 0 {
            return;
        }
        let layout = alloc::Layout::from_size_align_unchecked(size, align);
        alloc::dealloc(ptr, layout);
    }
    pub use alloc_crate::alloc;
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
        exports::testing::http_workflow::workflow::__export_testing_http_workflow_workflow_cabi!($ty
        with_types_in $($path_to_types_root)*::
        exports::testing::http_workflow::workflow);
    };
}
#[doc(inline)]
pub(crate) use __export_any_impl as export;
#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:wit-bindgen:0.31.0:testing:http-workflow:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 1247] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07\xe5\x08\x01A\x02\x01\
A\x10\x01B\x04\x01j\x01s\x01s\x01@\x01\x03urls\0\0\x04\0\x03get\x01\x01\x04\0\x0e\
get-successful\x01\x01\x03\x01\x15testing:http/http-get\x05\0\x01B\x0a\x04\0\x0b\
join-set-id\x03\x01\x01r\x01\x02ids\x04\0\x0cexecution-id\x03\0\x01\x01r\x01\x02\
ids\x04\0\x08delay-id\x03\0\x03\x01q\x03\x11permanent-failure\x01s\0\x11permanen\
t-timeout\0\0\x0enondeterminism\0\0\x04\0\x0fexecution-error\x03\0\x05\x01h\0\x01\
@\x01\x04self\x07\0s\x04\0\x16[method]join-set-id.id\x01\x08\x03\x01\x17obelisk:\
types/execution\x05\x01\x02\x03\0\x01\x0cexecution-id\x02\x03\0\x01\x0bjoin-set-\
id\x02\x03\0\x01\x0fexecution-error\x01B\x0f\x02\x03\x02\x01\x02\x04\0\x0cexecut\
ion-id\x03\0\0\x02\x03\x02\x01\x03\x04\0\x0bjoin-set-id\x03\0\x02\x02\x03\x02\x01\
\x04\x04\0\x0fexecution-error\x03\0\x04\x01h\x03\x01@\x02\x0bjoin-set-id\x06\x03\
urls\0\x01\x04\0\x15get-successful-submit\x01\x07\x01j\x01s\x01s\x01o\x02\x01\x08\
\x01o\x02\x01\x05\x01j\x01\x09\x01\x0a\x01@\x01\x0bjoin-set-id\x06\0\x0b\x04\0\x19\
get-successful-await-next\x01\x0c\x03\x01!testing:http-obelisk-ext/http-get\x05\x05\
\x01B\x06\x01q\x05\x0cmilliseconds\x01w\0\x07seconds\x01w\0\x07minutes\x01y\0\x05\
hours\x01y\0\x04days\x01y\0\x04\0\x08duration\x03\0\0\x01r\x02\x07secondsw\x0bna\
nosecondsy\x04\0\x08datetime\x03\0\x02\x01q\x03\x03now\0\0\x02at\x01\x03\0\x02in\
\x01\x01\0\x04\0\x0bschedule-at\x03\0\x04\x03\x01\x12obelisk:types/time\x05\x06\x02\
\x03\0\x03\x08duration\x01B\x09\x02\x03\x02\x01\x07\x04\0\x08duration\x03\0\0\x02\
\x03\x02\x01\x03\x04\0\x0bjoin-set-id\x03\0\x02\x01@\x01\x08duration\x01\x01\0\x04\
\0\x05sleep\x01\x04\x01i\x03\x01@\0\0\x05\x04\0\x0cnew-join-set\x01\x06\x03\x01\x20\
obelisk:workflow/host-activities\x05\x08\x01B\x0a\x01j\x01s\x01s\x01@\x01\x03url\
s\0\0\x04\0\x03get\x01\x01\x04\0\x0eget-successful\x01\x01\x01ps\x01j\x01\x02\x01\
s\x01@\x01\x04urls\x02\0\x03\x04\0\x1bget-successful-concurrently\x01\x04\x01@\x02\
\x03urls\x0bconcurrencyy\0\x03\x04\0\"get-successful-concurrently-stress\x01\x05\
\x04\x01\x1etesting:http-workflow/workflow\x05\x09\x04\x01\x19testing:http-workf\
low/any\x04\0\x0b\x09\x01\0\x03any\x03\0\0\0G\x09producers\x01\x0cprocessed-by\x02\
\x0dwit-component\x070.216.0\x10wit-bindgen-rust\x060.31.0";
#[inline(never)]
#[doc(hidden)]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
