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
}
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
                static __FORCE_SECTION_REF: fn() =
                    super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                pub type Duration = super::super::super::super::obelisk::types::time::Duration;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_sleep_cabi<T: Guest>(arg0: i32, arg1: i64) {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    use super::super::super::super::obelisk::types::time::Duration as V0;
                    let v0 = match arg0 {
                        0 => {
                            let e0 = arg1 as u64;
                            V0::Milliseconds(e0)
                        }
                        1 => {
                            let e0 = arg1 as u64;
                            V0::Seconds(e0)
                        }
                        2 => {
                            let e0 = arg1 as i32 as u32;
                            V0::Minutes(e0)
                        }
                        3 => {
                            let e0 = arg1 as i32 as u32;
                            V0::Hours(e0)
                        }
                        n => {
                            debug_assert_eq!(n, 4, "invalid enum discriminant");
                            let e0 = arg1 as i32 as u32;
                            V0::Days(e0)
                        }
                    };
                    T::sleep(v0);
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_sleep_loop_cabi<T: Guest>(arg0: i32, arg1: i64, arg2: i32) {
                    #[cfg(target_arch = "wasm32")]
                    _rt::run_ctors_once();
                    use super::super::super::super::obelisk::types::time::Duration as V0;
                    let v0 = match arg0 {
                        0 => {
                            let e0 = arg1 as u64;
                            V0::Milliseconds(e0)
                        }
                        1 => {
                            let e0 = arg1 as u64;
                            V0::Seconds(e0)
                        }
                        2 => {
                            let e0 = arg1 as i32 as u32;
                            V0::Minutes(e0)
                        }
                        3 => {
                            let e0 = arg1 as i32 as u32;
                            V0::Hours(e0)
                        }
                        n => {
                            debug_assert_eq!(n, 4, "invalid enum discriminant");
                            let e0 = arg1 as i32 as u32;
                            V0::Days(e0)
                        }
                    };
                    T::sleep_loop(v0, arg2 as u32);
                }
                pub trait Guest {
                    fn sleep(duration: Duration);
                    fn sleep_loop(duration: Duration, iterations: u32);
                }
                #[doc(hidden)]
                macro_rules! __export_testing_sleep_sleep_cabi {
                    ($ty:ident with_types_in $($path_to_types:tt)*) => {
                        const _ : () = { #[export_name = "testing:sleep/sleep#sleep"]
                        unsafe extern "C" fn export_sleep(arg0 : i32, arg1 : i64,) {
                        $($path_to_types)*:: _export_sleep_cabi::<$ty > (arg0, arg1) }
                        #[export_name = "testing:sleep/sleep#sleep-loop"] unsafe extern
                        "C" fn export_sleep_loop(arg0 : i32, arg1 : i64, arg2 : i32,) {
                        $($path_to_types)*:: _export_sleep_loop_cabi::<$ty > (arg0, arg1,
                        arg2) } };
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
#[link_section = "component-type:wit-bindgen:0.31.0:any:any:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 445] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07\xc3\x02\x01A\x02\x01\
A\x05\x01B\x06\x01q\x05\x0cmilliseconds\x01w\0\x07seconds\x01w\0\x07minutes\x01y\
\0\x05hours\x01y\0\x04days\x01y\0\x04\0\x08duration\x03\0\0\x01r\x02\x07secondsw\
\x0bnanosecondsy\x04\0\x08datetime\x03\0\x02\x01q\x03\x03now\0\0\x02at\x01\x03\0\
\x02in\x01\x01\0\x04\0\x0bschedule-at\x03\0\x04\x03\x01\x12obelisk:types/time\x05\
\0\x02\x03\0\0\x08duration\x01B\x06\x02\x03\x02\x01\x01\x04\0\x08duration\x03\0\0\
\x01@\x01\x08duration\x01\x01\0\x04\0\x05sleep\x01\x02\x01@\x02\x08duration\x01\x0a\
iterationsy\x01\0\x04\0\x0asleep-loop\x01\x03\x04\x01\x13testing:sleep/sleep\x05\
\x02\x04\x01\x0bany:any/any\x04\0\x0b\x09\x01\0\x03any\x03\0\0\0G\x09producers\x01\
\x0cprocessed-by\x02\x0dwit-component\x070.216.0\x10wit-bindgen-rust\x060.31.0";
#[inline(never)]
#[doc(hidden)]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
