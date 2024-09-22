#[allow(dead_code)]
pub mod exports {
    #[allow(dead_code)]
    pub mod testing {
        #[allow(dead_code)]
        pub mod sleep {
            #[allow(dead_code, clippy::all)]
            pub mod sleep {
                #[used]
                #[doc(hidden)]
                static __FORCE_SECTION_REF: fn() = super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_sleep_cabi<T: Guest>(arg0: i32) {
                    #[cfg(target_arch = "wasm32")] _rt::run_ctors_once();
                    T::sleep(arg0 as u32);
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_sleep_loop_cabi<T: Guest>(arg0: i32, arg1: i32) {
                    #[cfg(target_arch = "wasm32")] _rt::run_ctors_once();
                    T::sleep_loop(arg0 as u32, arg1 as u32);
                }
                pub trait Guest {
                    fn sleep(millis: u32);
                    fn sleep_loop(millis: u32, iterations: u32);
                }
                #[doc(hidden)]
                macro_rules! __export_testing_sleep_sleep_cabi {
                    ($ty:ident with_types_in $($path_to_types:tt)*) => {
                        const _ : () = { #[export_name = "testing:sleep/sleep#sleep"]
                        unsafe extern "C" fn export_sleep(arg0 : i32,) {
                        $($path_to_types)*:: _export_sleep_cabi::<$ty > (arg0) }
                        #[export_name = "testing:sleep/sleep#sleep-loop"] unsafe extern
                        "C" fn export_sleep_loop(arg0 : i32, arg1 : i32,) {
                        $($path_to_types)*:: _export_sleep_loop_cabi::<$ty > (arg0, arg1)
                        } };
                    };
                }
                #[doc(hidden)]
                pub(crate) use __export_testing_sleep_sleep_cabi;
            }
        }
    }
}
mod _rt {
    #[cfg(target_arch = "wasm32")]
    pub fn run_ctors_once() {
        wit_bindgen_rt::run_ctors_once();
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
        exports::testing::sleep::sleep::__export_testing_sleep_sleep_cabi!($ty
        with_types_in $($path_to_types_root)*:: exports::testing::sleep::sleep);
    };
}
#[doc(inline)]
pub(crate) use __export_any_impl as export;
#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:wit-bindgen:0.30.0:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 233] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07p\x01A\x02\x01A\x02\x01\
B\x04\x01@\x01\x06millisy\x01\0\x04\0\x05sleep\x01\0\x01@\x02\x06millisy\x0aiter\
ationsy\x01\0\x04\0\x0asleep-loop\x01\x01\x04\x01\x13testing:sleep/sleep\x05\0\x04\
\x01\x0bany:any/any\x04\0\x0b\x09\x01\0\x03any\x03\0\0\0G\x09producers\x01\x0cpr\
ocessed-by\x02\x0dwit-component\x070.215.0\x10wit-bindgen-rust\x060.30.0";
#[inline(never)]
#[doc(hidden)]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
