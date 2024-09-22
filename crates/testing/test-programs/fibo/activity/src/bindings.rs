#[allow(dead_code)]
pub mod exports {
    #[allow(dead_code)]
    pub mod testing {
        #[allow(dead_code)]
        pub mod fibo {
            #[allow(dead_code, clippy::all)]
            pub mod fibo {
                #[used]
                #[doc(hidden)]
                static __FORCE_SECTION_REF: fn() =
                    super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_fibo_cabi<T: Guest>(arg0: i32) -> i64 {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    let result0 = T::fibo(arg0 as u8);
                    _rt::as_i64(result0)
                }
                pub trait Guest {
                    fn fibo(n: u8) -> u64;
                }
                #[doc(hidden)]
                macro_rules! __export_testing_fibo_fibo_cabi {
                    ($ty:ident with_types_in $($path_to_types:tt)*) => {
                        const _ : () = { #[export_name = "testing:fibo/fibo#fibo"] unsafe
                        extern "C" fn export_fibo(arg0 : i32,) -> i64 {
                        $($path_to_types)*:: _export_fibo_cabi::<$ty > (arg0) } };
                    };
                }
                #[doc(hidden)]
                pub(crate) use __export_testing_fibo_fibo_cabi;
            }
        }
    }
}
mod _rt {
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
        exports::testing::fibo::fibo::__export_testing_fibo_fibo_cabi!($ty with_types_in
        $($path_to_types_root)*:: exports::testing::fibo::fibo);
    };
}
#[doc(inline)]
pub(crate) use __export_any_impl as export;
#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:wit-bindgen:0.30.0:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 185] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07@\x01A\x02\x01A\x02\x01\
B\x02\x01@\x01\x01n}\0w\x04\0\x04fibo\x01\0\x04\x01\x11testing:fibo/fibo\x05\0\x04\
\x01\x0bany:any/any\x04\0\x0b\x09\x01\0\x03any\x03\0\0\0G\x09producers\x01\x0cpr\
ocessed-by\x02\x0dwit-component\x070.215.0\x10wit-bindgen-rust\x060.30.0";
#[inline(never)]
#[doc(hidden)]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
