// Generated by `wit-bindgen` 0.24.0. DO NOT EDIT!
// Options used:
#[allow(dead_code)]
pub mod testing {
    #[allow(dead_code)]
    pub mod http {
        #[allow(dead_code, clippy::all)]
        pub mod http_get {
            #[used]
            #[doc(hidden)]
            #[cfg(target_arch = "wasm32")]
            static __FORCE_SECTION_REF: fn() =
                super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn get(authority: &str, path: &str) -> Result<_rt::String, _rt::String> {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                    let vec0 = authority;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let vec1 = path;
                    let ptr1 = vec1.as_ptr().cast::<u8>();
                    let len1 = vec1.len();
                    let ptr2 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:http/http-get")]
                    extern "C" {
                        #[link_name = "get"]
                        fn wit_import(_: *mut u8, _: usize, _: *mut u8, _: usize, _: *mut u8);
                    }

                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: *mut u8, _: usize, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0, ptr1.cast_mut(), len1, ptr2);
                    let l3 = i32::from(*ptr2.add(0).cast::<u8>());
                    match l3 {
                        0 => {
                            let e = {
                                let l4 = *ptr2.add(4).cast::<*mut u8>();
                                let l5 = *ptr2.add(8).cast::<usize>();
                                let len6 = l5;
                                let bytes6 = _rt::Vec::from_raw_parts(l4.cast(), len6, len6);

                                _rt::string_lift(bytes6)
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l7 = *ptr2.add(4).cast::<*mut u8>();
                                let l8 = *ptr2.add(8).cast::<usize>();
                                let len9 = l8;
                                let bytes9 = _rt::Vec::from_raw_parts(l7.cast(), len9, len9);

                                _rt::string_lift(bytes9)
                            };
                            Err(e)
                        }
                        _ => _rt::invalid_enum_discriminant(),
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn get_successful(authority: &str, path: &str) -> Result<_rt::String, _rt::String> {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 12]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 12]);
                    let vec0 = authority;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    let vec1 = path;
                    let ptr1 = vec1.as_ptr().cast::<u8>();
                    let len1 = vec1.len();
                    let ptr2 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:http/http-get")]
                    extern "C" {
                        #[link_name = "get-successful"]
                        fn wit_import(_: *mut u8, _: usize, _: *mut u8, _: usize, _: *mut u8);
                    }

                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize, _: *mut u8, _: usize, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0.cast_mut(), len0, ptr1.cast_mut(), len1, ptr2);
                    let l3 = i32::from(*ptr2.add(0).cast::<u8>());
                    match l3 {
                        0 => {
                            let e = {
                                let l4 = *ptr2.add(4).cast::<*mut u8>();
                                let l5 = *ptr2.add(8).cast::<usize>();
                                let len6 = l5;
                                let bytes6 = _rt::Vec::from_raw_parts(l4.cast(), len6, len6);

                                _rt::string_lift(bytes6)
                            };
                            Ok(e)
                        }
                        1 => {
                            let e = {
                                let l7 = *ptr2.add(4).cast::<*mut u8>();
                                let l8 = *ptr2.add(8).cast::<usize>();
                                let len9 = l8;
                                let bytes9 = _rt::Vec::from_raw_parts(l7.cast(), len9, len9);

                                _rt::string_lift(bytes9)
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
                #[cfg(target_arch = "wasm32")]
                static __FORCE_SECTION_REF: fn() =
                    super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_execute_cabi<T: Guest>(arg0: i32) -> *mut u8 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let result0 = T::execute(arg0 as u16);
                    let ptr1 = _RET_AREA.0.as_mut_ptr().cast::<u8>();
                    let vec2 = (result0.into_bytes()).into_boxed_slice();
                    let ptr2 = vec2.as_ptr().cast::<u8>();
                    let len2 = vec2.len();
                    ::core::mem::forget(vec2);
                    *ptr1.add(4).cast::<usize>() = len2;
                    *ptr1.add(0).cast::<*mut u8>() = ptr2.cast_mut();
                    ptr1
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn __post_return_execute<T: Guest>(arg0: *mut u8) {
                    let l0 = *arg0.add(0).cast::<*mut u8>();
                    let l1 = *arg0.add(4).cast::<usize>();
                    _rt::cabi_dealloc(l0, l1, 1);
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_get_cabi<T: Guest>(
                    arg0: *mut u8,
                    arg1: usize,
                    arg2: *mut u8,
                    arg3: usize,
                ) -> *mut u8 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let len0 = arg1;
                    let bytes0 = _rt::Vec::from_raw_parts(arg0.cast(), len0, len0);
                    let len1 = arg3;
                    let bytes1 = _rt::Vec::from_raw_parts(arg2.cast(), len1, len1);
                    let result2 = T::get(_rt::string_lift(bytes0), _rt::string_lift(bytes1));
                    let ptr3 = _RET_AREA.0.as_mut_ptr().cast::<u8>();
                    match result2 {
                        Ok(e) => {
                            *ptr3.add(0).cast::<u8>() = (0i32) as u8;
                            let vec4 = (e.into_bytes()).into_boxed_slice();
                            let ptr4 = vec4.as_ptr().cast::<u8>();
                            let len4 = vec4.len();
                            ::core::mem::forget(vec4);
                            *ptr3.add(8).cast::<usize>() = len4;
                            *ptr3.add(4).cast::<*mut u8>() = ptr4.cast_mut();
                        }
                        Err(e) => {
                            *ptr3.add(0).cast::<u8>() = (1i32) as u8;
                            let vec5 = (e.into_bytes()).into_boxed_slice();
                            let ptr5 = vec5.as_ptr().cast::<u8>();
                            let len5 = vec5.len();
                            ::core::mem::forget(vec5);
                            *ptr3.add(8).cast::<usize>() = len5;
                            *ptr3.add(4).cast::<*mut u8>() = ptr5.cast_mut();
                        }
                    };
                    ptr3
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
                pub trait Guest {
                    fn execute(port: u16) -> _rt::String;
                    fn get(
                        authority: _rt::String,
                        path: _rt::String,
                    ) -> Result<_rt::String, _rt::String>;
                }
                #[doc(hidden)]

                macro_rules! __export_testing_http_workflow_workflow_cabi{
      ($ty:ident with_types_in $($path_to_types:tt)*) => (const _: () = {

        #[export_name = "testing:http-workflow/workflow#execute"]
        unsafe extern "C" fn export_execute(arg0: i32,) -> *mut u8 {
          $($path_to_types)*::_export_execute_cabi::<$ty>(arg0)
        }
        #[export_name = "cabi_post_testing:http-workflow/workflow#execute"]
        unsafe extern "C" fn _post_return_execute(arg0: *mut u8,) {
          $($path_to_types)*::__post_return_execute::<$ty>(arg0)
        }
        #[export_name = "testing:http-workflow/workflow#get"]
        unsafe extern "C" fn export_get(arg0: *mut u8,arg1: usize,arg2: *mut u8,arg3: usize,) -> *mut u8 {
          $($path_to_types)*::_export_get_cabi::<$ty>(arg0, arg1, arg2, arg3)
        }
        #[export_name = "cabi_post_testing:http-workflow/workflow#get"]
        unsafe extern "C" fn _post_return_get(arg0: *mut u8,) {
          $($path_to_types)*::__post_return_get::<$ty>(arg0)
        }
      };);
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

    #[cfg(target_arch = "wasm32")]
    pub fn run_ctors_once() {
        wit_bindgen_rt::run_ctors_once();
    }
    pub unsafe fn cabi_dealloc(ptr: *mut u8, size: usize, align: usize) {
        if size == 0 {
            return;
        }
        let layout = alloc::Layout::from_size_align_unchecked(size, align);
        alloc::dealloc(ptr as *mut u8, layout);
    }
    extern crate alloc as alloc_crate;
    pub use alloc_crate::alloc;
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
  ($ty:ident) => (self::export!($ty with_types_in self););
  ($ty:ident with_types_in $($path_to_types_root:tt)*) => (
  $($path_to_types_root)*::exports::testing::http_workflow::workflow::__export_testing_http_workflow_workflow_cabi!($ty with_types_in $($path_to_types_root)*::exports::testing::http_workflow::workflow);
  )
}
#[doc(inline)]
pub(crate) use __export_any_impl as export;

#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:wit-bindgen:0.24.0:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 339] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07\xd9\x01\x01A\x02\x01\
A\x04\x01B\x04\x01j\x01s\x01s\x01@\x02\x09authoritys\x04paths\0\0\x04\0\x03get\x01\
\x01\x04\0\x0eget-successful\x01\x01\x03\x01\x15testing:http/http-get\x05\0\x01B\
\x05\x01@\x01\x04port{\0s\x04\0\x07execute\x01\0\x01j\x01s\x01s\x01@\x02\x09auth\
oritys\x04paths\0\x01\x04\0\x03get\x01\x02\x04\x01\x1etesting:http-workflow/work\
flow\x05\x01\x04\x01\x19testing:http-workflow/any\x04\0\x0b\x09\x01\0\x03any\x03\
\0\0\0G\x09producers\x01\x0cprocessed-by\x02\x0dwit-component\x070.202.0\x10wit-\
bindgen-rust\x060.24.0";

#[inline(never)]
#[doc(hidden)]
#[cfg(target_arch = "wasm32")]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
