#[allow(dead_code)]
pub mod obelisk {
    #[allow(dead_code)]
    pub mod workflow {
        #[allow(dead_code, clippy::all)]
        pub mod host_activities {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() = super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn sleep(millis: u32) {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "obelisk:workflow/host-activities")]
                    extern "C" {
                        #[link_name = "sleep"]
                        fn wit_import(_: i32);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32) {
                        unreachable!()
                    }
                    wit_import(_rt::as_i32(&millis));
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            pub fn new_join_set() -> _rt::String {
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
            static __FORCE_SECTION_REF: fn() = super::super::super::__link_custom_section_describing_imports;
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
            static __FORCE_SECTION_REF: fn() = super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
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
            pub fn fibo_await_next(join_set_id: &str) -> u64 {
                unsafe {
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:fibo-obelisk-ext/fibo")]
                    extern "C" {
                        #[link_name = "fibo-await-next"]
                        fn wit_import(_: *mut u8, _: usize) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(ptr0.cast_mut(), len0);
                    ret as u64
                }
            }
        }
    }
    #[allow(dead_code)]
    pub mod fibo_workflow {
        #[allow(dead_code, clippy::all)]
        pub mod workflow {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() = super::super::super::__link_custom_section_describing_imports;
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
        #[allow(dead_code, clippy::all)]
        pub mod workflow_nesting {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() = super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fibo_nested_workflow(n: u8) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(
                        wasm_import_module = "testing:fibo-workflow/workflow-nesting"
                    )]
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
            pub fn fibo_start_fiboas(
                n: u8,
                fiboas: u32,
                iterations_per_fiboa: u32,
            ) -> u64 {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(
                        wasm_import_module = "testing:fibo-workflow/workflow-nesting"
                    )]
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
    }
    #[allow(dead_code)]
    pub mod fibo_workflow_obelisk_ext {
        #[allow(dead_code, clippy::all)]
        pub mod workflow {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() = super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn fiboa_submit(
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
                    #[link(
                        wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow"
                    )]
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
            pub fn fiboa_await_next(join_set_id: &str) -> u64 {
                unsafe {
                    let vec0 = join_set_id;
                    let ptr0 = vec0.as_ptr().cast::<u8>();
                    let len0 = vec0.len();
                    #[cfg(target_arch = "wasm32")]
                    #[link(
                        wasm_import_module = "testing:fibo-workflow-obelisk-ext/workflow"
                    )]
                    extern "C" {
                        #[link_name = "fiboa-await-next"]
                        fn wit_import(_: *mut u8, _: usize) -> i64;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8, _: usize) -> i64 {
                        unreachable!()
                    }
                    let ret = wit_import(ptr0.cast_mut(), len0);
                    ret as u64
                }
            }
        }
    }
}
#[allow(dead_code)]
pub mod wasi {
    #[allow(dead_code)]
    pub mod clocks {
        #[allow(dead_code, clippy::all)]
        pub mod wall_clock {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() = super::super::super::__link_custom_section_describing_imports;
            /// A time and date in seconds plus nanoseconds.
            #[repr(C)]
            #[derive(Clone, Copy)]
            pub struct Datetime {
                pub seconds: u64,
                pub nanoseconds: u32,
            }
            impl ::core::fmt::Debug for Datetime {
                fn fmt(
                    &self,
                    f: &mut ::core::fmt::Formatter<'_>,
                ) -> ::core::fmt::Result {
                    f.debug_struct("Datetime")
                        .field("seconds", &self.seconds)
                        .field("nanoseconds", &self.nanoseconds)
                        .finish()
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            /// Read the current value of the clock.
            ///
            /// This clock is not monotonic, therefore calling this function repeatedly
            /// will not necessarily produce a sequence of non-decreasing values.
            ///
            /// The returned timestamps represent the number of seconds since
            /// 1970-01-01T00:00:00Z, also known as [POSIX's Seconds Since the Epoch],
            /// also known as [Unix Time].
            ///
            /// The nanoseconds field of the output is always less than 1000000000.
            ///
            /// [POSIX's Seconds Since the Epoch]: https://pubs.opengroup.org/onlinepubs/9699919799/xrat/V4_xbd_chap04.html#tag_21_04_16
            /// [Unix Time]: https://en.wikipedia.org/wiki/Unix_time
            pub fn now() -> Datetime {
                unsafe {
                    #[repr(align(8))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "wasi:clocks/wall-clock@0.2.0")]
                    extern "C" {
                        #[link_name = "now"]
                        fn wit_import(_: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0);
                    let l1 = *ptr0.add(0).cast::<i64>();
                    let l2 = *ptr0.add(8).cast::<i32>();
                    Datetime {
                        seconds: l1 as u64,
                        nanoseconds: l2 as u32,
                    }
                }
            }
            #[allow(unused_unsafe, clippy::all)]
            /// Query the resolution of the clock.
            ///
            /// The nanoseconds field of the output is always less than 1000000000.
            pub fn resolution() -> Datetime {
                unsafe {
                    #[repr(align(8))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 16]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 16]);
                    let ptr0 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "wasi:clocks/wall-clock@0.2.0")]
                    extern "C" {
                        #[link_name = "resolution"]
                        fn wit_import(_: *mut u8);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: *mut u8) {
                        unreachable!()
                    }
                    wit_import(ptr0);
                    let l1 = *ptr0.add(0).cast::<i64>();
                    let l2 = *ptr0.add(8).cast::<i32>();
                    Datetime {
                        seconds: l1 as u64,
                        nanoseconds: l2 as u32,
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
            pub mod workflow {
                #[used]
                #[doc(hidden)]
                static __FORCE_SECTION_REF: fn() = super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_fibow_cabi<T: Guest>(arg0: i32, arg1: i32) -> i64 {
                    #[cfg(target_arch = "wasm32")] _rt::run_ctors_once();
                    let result0 = T::fibow(arg0 as u8, arg1 as u32);
                    _rt::as_i64(result0)
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_fiboa_cabi<T: Guest>(arg0: i32, arg1: i32) -> i64 {
                    #[cfg(target_arch = "wasm32")] _rt::run_ctors_once();
                    let result0 = T::fiboa(arg0 as u8, arg1 as u32);
                    _rt::as_i64(result0)
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_fiboa_concurrent_cabi<T: Guest>(
                    arg0: i32,
                    arg1: i32,
                ) -> i64 {
                    #[cfg(target_arch = "wasm32")] _rt::run_ctors_once();
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
            #[allow(dead_code, clippy::all)]
            pub mod workflow_nesting {
                #[used]
                #[doc(hidden)]
                static __FORCE_SECTION_REF: fn() = super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_fibo_nested_workflow_cabi<T: Guest>(
                    arg0: i32,
                ) -> i64 {
                    #[cfg(target_arch = "wasm32")] _rt::run_ctors_once();
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
                    #[cfg(target_arch = "wasm32")] _rt::run_ctors_once();
                    let result0 = T::fibo_start_fiboas(
                        arg0 as u8,
                        arg1 as u32,
                        arg2 as u32,
                    );
                    _rt::as_i64(result0)
                }
                pub trait Guest {
                    fn fibo_nested_workflow(n: u8) -> u64;
                    fn fibo_start_fiboas(
                        n: u8,
                        fiboas: u32,
                        iterations_per_fiboa: u32,
                    ) -> u64;
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
        }
    }
}
mod _rt {
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
    pub use alloc_crate::string::String;
    pub use alloc_crate::vec::Vec;
    pub unsafe fn string_lift(bytes: Vec<u8>) -> String {
        if cfg!(debug_assertions) {
            String::from_utf8(bytes).unwrap()
        } else {
            String::from_utf8_unchecked(bytes)
        }
    }
    #[cfg(target_arch = "wasm32")]
    pub fn run_ctors_once() {
        wit_bindgen_rt::run_ctors_once();
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
        exports::testing::fibo_workflow::workflow::__export_testing_fibo_workflow_workflow_cabi!($ty
        with_types_in $($path_to_types_root)*::
        exports::testing::fibo_workflow::workflow); $($path_to_types_root)*::
        exports::testing::fibo_workflow::workflow_nesting::__export_testing_fibo_workflow_workflow_nesting_cabi!($ty
        with_types_in $($path_to_types_root)*::
        exports::testing::fibo_workflow::workflow_nesting);
    };
}
#[doc(inline)]
pub(crate) use __export_any_impl as export;
#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:wit-bindgen:0.30.0:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 1188] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07\xaa\x08\x01A\x02\x01\
A\x13\x01B\x02\x01@\x01\x01n}\0w\x04\0\x04fibo\x01\0\x03\x01\x11testing:fibo/fib\
o\x05\0\x01B\x05\x01r\x02\x07secondsw\x0bnanosecondsy\x04\0\x08datetime\x03\0\0\x01\
@\0\0\x01\x04\0\x03now\x01\x02\x04\0\x0aresolution\x01\x02\x03\x01\x1cwasi:clock\
s/wall-clock@0.2.0\x05\x01\x02\x03\0\x01\x08datetime\x01B\x0a\x02\x03\x02\x01\x02\
\x04\0\x08datetime\x03\0\0\x01w\x04\0\x08duration\x03\0\x02\x01q\x03\x03now\0\0\x02\
at\x01\x01\0\x02in\x01\x03\0\x04\0\x0cscheduled-at\x03\0\x04\x01@\x01\x06millisy\
\x01\0\x04\0\x05sleep\x01\x06\x01@\0\0s\x04\0\x0cnew-join-set\x01\x07\x03\x01\x20\
obelisk:workflow/host-activities\x05\x03\x01B\x04\x01@\x02\x0bjoin-set-ids\x01n}\
\0s\x04\0\x0bfibo-submit\x01\0\x01@\x01\x0bjoin-set-ids\0w\x04\0\x0ffibo-await-n\
ext\x01\x01\x03\x01\x1dtesting:fibo-obelisk-ext/fibo\x05\x04\x01B\x04\x01@\x02\x01\
n}\x0aiterationsy\0w\x04\0\x05fibow\x01\0\x04\0\x05fiboa\x01\0\x04\0\x10fiboa-co\
ncurrent\x01\0\x03\x01\x1etesting:fibo-workflow/workflow\x05\x05\x01B\x04\x01@\x01\
\x01n}\0w\x04\0\x14fibo-nested-workflow\x01\0\x01@\x03\x01n}\x06fiboasy\x14itera\
tions-per-fiboay\0w\x04\0\x11fibo-start-fiboas\x01\x01\x03\x01&testing:fibo-work\
flow/workflow-nesting\x05\x06\x01B\x04\x01@\x03\x0bjoin-set-ids\x01n}\x0aiterati\
onsy\0s\x04\0\x0cfiboa-submit\x01\0\x01@\x01\x0bjoin-set-ids\0w\x04\0\x10fiboa-a\
wait-next\x01\x01\x03\x01*testing:fibo-workflow-obelisk-ext/workflow\x05\x07\x01\
B\x04\x01@\x02\x01n}\x0aiterationsy\0w\x04\0\x05fibow\x01\0\x04\0\x05fiboa\x01\0\
\x04\0\x10fiboa-concurrent\x01\0\x04\x01\x1etesting:fibo-workflow/workflow\x05\x08\
\x01B\x04\x01@\x01\x01n}\0w\x04\0\x14fibo-nested-workflow\x01\0\x01@\x03\x01n}\x06\
fiboasy\x14iterations-per-fiboay\0w\x04\0\x11fibo-start-fiboas\x01\x01\x04\x01&t\
esting:fibo-workflow/workflow-nesting\x05\x09\x04\x01\x0bany:any/any\x04\0\x0b\x09\
\x01\0\x03any\x03\0\0\0G\x09producers\x01\x0cprocessed-by\x02\x0dwit-component\x07\
0.215.0\x10wit-bindgen-rust\x060.30.0";
#[inline(never)]
#[doc(hidden)]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
