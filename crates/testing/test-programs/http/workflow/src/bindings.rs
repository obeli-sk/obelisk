#[allow(dead_code)]
pub mod obelisk {
    #[allow(dead_code)]
    pub mod types {
        #[allow(dead_code, clippy::all)]
        pub mod time {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            /// A duration of time, in nanoseconds.
            /// Extracted from wasi:clocks@0.2.0 to avoid dependency on wasi:io
            pub type Duration = u64;
        }
        #[allow(dead_code, clippy::all)]
        pub mod execution {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type JoinSetId = _rt::String;
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
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:workflow/host-activities")]
                    extern "C" {
                        #[link_name = "sleep"]
                        fn wit_import(_: i64);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i64) {
                        unreachable!()
                    }
                    wit_import(_rt::as_i64(nanos));
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
            #[allow(unused_unsafe, clippy::all)]
            pub fn get_successful_submit(join_set_id: &str, url: &str) -> _rt::String {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let vec1 = url;
                    let ptr1 = vec1.as_ptr().cast::<u8>();
                    let len1 = vec1.len();
                    let ptr2 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:http-obelisk-ext/http-get")]
                    extern "C" {
                        #[link_name = "get-successful-submit"]
                        fn wit_import(_: *mut u8, _: usize, _: *mut u8, _: usize, _: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: *mut u8, _: usize, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0, ptr1.cast_mut(), len1, ptr2);
                    let l3 = *ptr2.add(0).cast::<*mut u8>();
                    let l4 = *ptr2.add(4).cast::<usize>();
                    let len5 = l4;
                    let bytes5 = _rt::Vec::from_raw_parts(l3.cast(), len5, len5);
                    _rt::string_lift(bytes5)
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn get_successful_await_next(
                join_set_id: &str,
            ) -> Result<_rt::String, _rt::String> {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let ptr1 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:http-obelisk-ext/http-get")]
                    extern "C" {
                        #[link_name = "get-successful-await-next"]
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
                                {
                                    ::core::ptr::null_mut()
                                }
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
                                {
                                    ::core::ptr::null_mut()
                                }
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
#[link_section = "component-type:wit-bindgen:0.30.0:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 912] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07\x96\x06\x01A\x02\x01\
A\x0e\x01B\x04\x01j\x01s\x01s\x01@\x01\x03urls\0\0\x04\0\x03get\x01\x01\x04\0\x0e\
get-successful\x01\x01\x03\x01\x15testing:http/http-get\x05\0\x01B\x05\x01@\x02\x0b\
join-set-ids\x03urls\0s\x04\0\x15get-successful-submit\x01\0\x01j\x01s\x01s\x01@\
\x01\x0bjoin-set-ids\0\x01\x04\0\x19get-successful-await-next\x01\x02\x03\x01!te\
sting:http-obelisk-ext/http-get\x05\x01\x01B\x06\x01w\x04\0\x08duration\x03\0\0\x01\
r\x02\x07secondsw\x0bnanosecondsy\x04\0\x08datetime\x03\0\x02\x01q\x03\x03now\0\0\
\x02at\x01\x03\0\x02in\x01\x01\0\x04\0\x0bschedule-at\x03\0\x04\x03\x01\x12obeli\
sk:types/time\x05\x02\x01B\x04\x01s\x04\0\x0bjoin-set-id\x03\0\0\x01s\x04\0\x0ce\
xecution-id\x03\0\x02\x03\x01\x17obelisk:types/execution\x05\x03\x02\x03\0\x02\x08\
duration\x02\x03\0\x03\x0bjoin-set-id\x01B\x08\x02\x03\x02\x01\x04\x04\0\x08dura\
tion\x03\0\0\x02\x03\x02\x01\x05\x04\0\x0bjoin-set-id\x03\0\x02\x01@\x01\x05nano\
s\x01\x01\0\x04\0\x05sleep\x01\x04\x01@\0\0\x03\x04\0\x0cnew-join-set\x01\x05\x03\
\x01\x20obelisk:workflow/host-activities\x05\x06\x01B\x0a\x01j\x01s\x01s\x01@\x01\
\x03urls\0\0\x04\0\x03get\x01\x01\x04\0\x0eget-successful\x01\x01\x01ps\x01j\x01\
\x02\x01s\x01@\x01\x04urls\x02\0\x03\x04\0\x1bget-successful-concurrently\x01\x04\
\x01@\x02\x03urls\x0bconcurrencyy\0\x03\x04\0\"get-successful-concurrently-stres\
s\x01\x05\x04\x01\x1etesting:http-workflow/workflow\x05\x07\x04\x01\x19testing:h\
ttp-workflow/any\x04\0\x0b\x09\x01\0\x03any\x03\0\0\0G\x09producers\x01\x0cproce\
ssed-by\x02\x0dwit-component\x070.215.0\x10wit-bindgen-rust\x060.30.0";
#[inline(never)]
#[doc(hidden)]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
