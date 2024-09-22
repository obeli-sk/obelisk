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
            pub type Datetime = super::super::super::wasi::clocks::wall_clock::Datetime;
            /// A duration of time, in nanoseconds.
            pub type Duration = u64;
            #[derive(Clone, Copy)]
            pub enum ScheduledAt {
                Now,
                At(Datetime),
                In(Duration),
            }
            impl ::core::fmt::Debug for ScheduledAt {
                fn fmt(
                    &self,
                    f: &mut ::core::fmt::Formatter<'_>,
                ) -> ::core::fmt::Result {
                    match self {
                        ScheduledAt::Now => f.debug_tuple("ScheduledAt::Now").finish(),
                        ScheduledAt::At(e) => {
                            f.debug_tuple("ScheduledAt::At").field(e).finish()
                        }
                        ScheduledAt::In(e) => {
                            f.debug_tuple("ScheduledAt::In").field(e).finish()
                        }
                    }
                }
            }
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
    pub mod sleep {
        #[allow(dead_code, clippy::all)]
        pub mod sleep {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() = super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            #[allow(unused_unsafe, clippy::all)]
            pub fn sleep(millis: u32) {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:sleep/sleep")]
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
            pub fn sleep_loop(millis: u32, iterations: u32) {
                unsafe {
                    #[cfg(target_arch = "wasm32")]
                    #[link(wasm_import_module = "testing:sleep/sleep")]
                    extern "C" {
                        #[link_name = "sleep-loop"]
                        fn wit_import(_: i32, _: i32);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i32) {
                        unreachable!()
                    }
                    wit_import(_rt::as_i32(&millis), _rt::as_i32(&iterations));
                }
            }
        }
    }
    #[allow(dead_code)]
    pub mod sleep_workflow_obelisk_ext {
        #[allow(dead_code, clippy::all)]
        pub mod workflow {
            #[used]
            #[doc(hidden)]
            static __FORCE_SECTION_REF: fn() = super::super::super::__link_custom_section_describing_imports;
            use super::super::super::_rt;
            pub type ScheduledAt = super::super::super::obelisk::workflow::host_activities::ScheduledAt;
            pub type Duration = super::super::super::obelisk::workflow::host_activities::Duration;
            #[allow(unused_unsafe, clippy::all)]
            pub fn reschedule_schedule(
                schedule: ScheduledAt,
                nanos: Duration,
                iterations: u8,
            ) -> _rt::String {
                unsafe {
                    #[repr(align(4))]
                    struct RetArea([::core::mem::MaybeUninit<u8>; 8]);
                    let mut ret_area = RetArea([::core::mem::MaybeUninit::uninit(); 8]);
                    use super::super::super::obelisk::workflow::host_activities::ScheduledAt as V1;
                    let (result2_0, result2_1, result2_2) = match schedule {
                        V1::Now => (0i32, 0i64, 0i32),
                        V1::At(e) => {
                            let super::super::super::wasi::clocks::wall_clock::Datetime {
                                seconds: seconds0,
                                nanoseconds: nanoseconds0,
                            } = e;
                            (1i32, _rt::as_i64(seconds0), _rt::as_i32(nanoseconds0))
                        }
                        V1::In(e) => (2i32, _rt::as_i64(e), 0i32),
                    };
                    let ptr3 = ret_area.0.as_mut_ptr().cast::<u8>();
                    #[cfg(target_arch = "wasm32")]
                    #[link(
                        wasm_import_module = "testing:sleep-workflow-obelisk-ext/workflow"
                    )]
                    extern "C" {
                        #[link_name = "reschedule-schedule"]
                        fn wit_import(
                            _: i32,
                            _: i64,
                            _: i32,
                            _: i64,
                            _: i32,
                            _: *mut u8,
                        );
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    fn wit_import(_: i32, _: i64, _: i32, _: i64, _: i32, _: *mut u8) {
                        unreachable!()
                    }
                    wit_import(
                        result2_0,
                        result2_1,
                        result2_2,
                        _rt::as_i64(nanos),
                        _rt::as_i32(&iterations),
                        ptr3,
                    );
                    let l4 = *ptr3.add(0).cast::<*mut u8>();
                    let l5 = *ptr3.add(4).cast::<usize>();
                    let len6 = l5;
                    let bytes6 = _rt::Vec::from_raw_parts(l4.cast(), len6, len6);
                    _rt::string_lift(bytes6)
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
        pub mod sleep_workflow {
            #[allow(dead_code, clippy::all)]
            pub mod workflow {
                #[used]
                #[doc(hidden)]
                static __FORCE_SECTION_REF: fn() = super::super::super::super::__link_custom_section_describing_imports;
                use super::super::super::super::_rt;
                pub type Duration = super::super::super::super::obelisk::workflow::host_activities::Duration;
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_sleep_host_activity_cabi<T: Guest>(arg0: i32) {
                    #[cfg(target_arch = "wasm32")] _rt::run_ctors_once();
                    T::sleep_host_activity(arg0 as u32);
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_sleep_activity_cabi<T: Guest>(arg0: i32) {
                    #[cfg(target_arch = "wasm32")] _rt::run_ctors_once();
                    T::sleep_activity(arg0 as u32);
                }
                #[doc(hidden)]
                #[allow(non_snake_case)]
                pub unsafe fn _export_reschedule_cabi<T: Guest>(arg0: i64, arg1: i32) {
                    #[cfg(target_arch = "wasm32")] _rt::run_ctors_once();
                    T::reschedule(arg0 as u64, arg1 as u8);
                }
                pub trait Guest {
                    fn sleep_host_activity(millis: u32);
                    fn sleep_activity(millis: u32);
                    fn reschedule(nanos: Duration, iterations: u8);
                }
                #[doc(hidden)]
                macro_rules! __export_testing_sleep_workflow_workflow_cabi {
                    ($ty:ident with_types_in $($path_to_types:tt)*) => {
                        const _ : () = { #[export_name =
                        "testing:sleep-workflow/workflow#sleep-host-activity"] unsafe
                        extern "C" fn export_sleep_host_activity(arg0 : i32,) {
                        $($path_to_types)*:: _export_sleep_host_activity_cabi::<$ty >
                        (arg0) } #[export_name =
                        "testing:sleep-workflow/workflow#sleep-activity"] unsafe extern
                        "C" fn export_sleep_activity(arg0 : i32,) { $($path_to_types)*::
                        _export_sleep_activity_cabi::<$ty > (arg0) } #[export_name =
                        "testing:sleep-workflow/workflow#reschedule"] unsafe extern "C"
                        fn export_reschedule(arg0 : i64, arg1 : i32,) {
                        $($path_to_types)*:: _export_reschedule_cabi::<$ty > (arg0, arg1)
                        } };
                    };
                }
                #[doc(hidden)]
                pub(crate) use __export_testing_sleep_workflow_workflow_cabi;
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
        exports::testing::sleep_workflow::workflow::__export_testing_sleep_workflow_workflow_cabi!($ty
        with_types_in $($path_to_types_root)*::
        exports::testing::sleep_workflow::workflow);
    };
}
#[doc(inline)]
pub(crate) use __export_any_impl as export;
#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:wit-bindgen:0.30.0:any:encoded world"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 843] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07\xd1\x05\x01A\x02\x01\
A\x0d\x01B\x05\x01r\x02\x07secondsw\x0bnanosecondsy\x04\0\x08datetime\x03\0\0\x01\
@\0\0\x01\x04\0\x03now\x01\x02\x04\0\x0aresolution\x01\x02\x03\x01\x1cwasi:clock\
s/wall-clock@0.2.0\x05\0\x02\x03\0\0\x08datetime\x01B\x0a\x02\x03\x02\x01\x01\x04\
\0\x08datetime\x03\0\0\x01w\x04\0\x08duration\x03\0\x02\x01q\x03\x03now\0\0\x02a\
t\x01\x01\0\x02in\x01\x03\0\x04\0\x0cscheduled-at\x03\0\x04\x01@\x01\x06millisy\x01\
\0\x04\0\x05sleep\x01\x06\x01@\0\0s\x04\0\x0cnew-join-set\x01\x07\x03\x01\x20obe\
lisk:workflow/host-activities\x05\x02\x01B\x04\x01@\x01\x06millisy\x01\0\x04\0\x05\
sleep\x01\0\x01@\x02\x06millisy\x0aiterationsy\x01\0\x04\0\x0asleep-loop\x01\x01\
\x03\x01\x13testing:sleep/sleep\x05\x03\x02\x03\0\x01\x0cscheduled-at\x02\x03\0\x01\
\x08duration\x01B\x06\x02\x03\x02\x01\x04\x04\0\x0cscheduled-at\x03\0\0\x02\x03\x02\
\x01\x05\x04\0\x08duration\x03\0\x02\x01@\x03\x08schedule\x01\x05nanos\x03\x0ait\
erations}\0s\x04\0\x13reschedule-schedule\x01\x04\x03\x01+testing:sleep-workflow\
-obelisk-ext/workflow\x05\x06\x01B\x07\x02\x03\x02\x01\x05\x04\0\x08duration\x03\
\0\0\x01@\x01\x06millisy\x01\0\x04\0\x13sleep-host-activity\x01\x02\x04\0\x0esle\
ep-activity\x01\x02\x01@\x02\x05nanos\x01\x0aiterations}\x01\0\x04\0\x0areschedu\
le\x01\x03\x04\x01\x1ftesting:sleep-workflow/workflow\x05\x07\x04\x01\x0bany:any\
/any\x04\0\x0b\x09\x01\0\x03any\x03\0\0\0G\x09producers\x01\x0cprocessed-by\x02\x0d\
wit-component\x070.215.0\x10wit-bindgen-rust\x060.30.0";
#[inline(never)]
#[doc(hidden)]
pub fn __link_custom_section_describing_imports() {
    wit_bindgen_rt::maybe_link_cabi_realloc();
}
