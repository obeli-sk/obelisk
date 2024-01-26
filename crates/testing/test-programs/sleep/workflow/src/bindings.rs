#![allow(dead_code)]
// Generated by `wit-bindgen` 0.16.0. DO NOT EDIT!
pub mod my_org {
  pub mod workflow_engine {
    
    #[allow(clippy::all)]
    pub mod host_activities {
      #[used]
      #[doc(hidden)]
      #[cfg(target_arch = "wasm32")]
      static __FORCE_SECTION_REF: fn() = super::super::super::__link_section;
      #[allow(unused_unsafe, clippy::all)]
      pub fn sleep(millis: u64,){
        
        #[allow(unused_imports)]
        use wit_bindgen::rt::{alloc, vec::Vec, string::String};
        unsafe {
          
          #[cfg(target_arch = "wasm32")]
          #[link(wasm_import_module = "my-org:workflow-engine/host-activities")]
          extern "C" {
            #[link_name = "sleep"]
            fn wit_import(_: i64, );
          }
          
          #[cfg(not(target_arch = "wasm32"))]
          fn wit_import(_: i64, ){ unreachable!() }
          wit_import(wit_bindgen::rt::as_i64(millis));
        }
      }
      #[allow(unused_unsafe, clippy::all)]
      pub fn noop(){
        
        #[allow(unused_imports)]
        use wit_bindgen::rt::{alloc, vec::Vec, string::String};
        unsafe {
          
          #[cfg(target_arch = "wasm32")]
          #[link(wasm_import_module = "my-org:workflow-engine/host-activities")]
          extern "C" {
            #[link_name = "noop"]
            fn wit_import();
          }
          
          #[cfg(not(target_arch = "wasm32"))]
          fn wit_import(){ unreachable!() }
          wit_import();
        }
      }
      
    }
    
  }
}
pub mod testing {
  pub mod sleep {
    
    #[allow(clippy::all)]
    pub mod sleep {
      #[used]
      #[doc(hidden)]
      #[cfg(target_arch = "wasm32")]
      static __FORCE_SECTION_REF: fn() = super::super::super::__link_section;
      #[allow(unused_unsafe, clippy::all)]
      pub fn sleep(millis: u64,){
        
        #[allow(unused_imports)]
        use wit_bindgen::rt::{alloc, vec::Vec, string::String};
        unsafe {
          
          #[cfg(target_arch = "wasm32")]
          #[link(wasm_import_module = "testing:sleep/sleep")]
          extern "C" {
            #[link_name = "sleep"]
            fn wit_import(_: i64, );
          }
          
          #[cfg(not(target_arch = "wasm32"))]
          fn wit_import(_: i64, ){ unreachable!() }
          wit_import(wit_bindgen::rt::as_i64(millis));
        }
      }
      
    }
    
  }
}
pub mod wasi {
  pub mod cli {
    
    #[allow(clippy::all)]
    pub mod run {
      #[used]
      #[doc(hidden)]
      #[cfg(target_arch = "wasm32")]
      static __FORCE_SECTION_REF: fn() = super::super::super::__link_section;
      #[allow(unused_unsafe, clippy::all)]
      /// Run the program.
      pub fn run() -> Result<(),()>{
        
        #[allow(unused_imports)]
        use wit_bindgen::rt::{alloc, vec::Vec, string::String};
        unsafe {
          
          #[cfg(target_arch = "wasm32")]
          #[link(wasm_import_module = "wasi:cli/run@0.2.0")]
          extern "C" {
            #[link_name = "run"]
            fn wit_import() -> i32;
          }
          
          #[cfg(not(target_arch = "wasm32"))]
          fn wit_import() -> i32{ unreachable!() }
          let ret = wit_import();
          match ret {
            0 => {
              let e = ();
              Ok(e)
            }
            1 => {
              let e = ();
              Err(e)
            }
            _ => wit_bindgen::rt::invalid_enum_discriminant(),
          }
        }
      }
      
    }
    
  }
}
pub mod exports {
  pub mod testing {
    pub mod sleep_workflow {
      
      #[allow(clippy::all)]
      pub mod workflow {
        #[used]
        #[doc(hidden)]
        #[cfg(target_arch = "wasm32")]
        static __FORCE_SECTION_REF: fn() = super::super::super::super::__link_section;
        const _: () = {
          
          #[doc(hidden)]
          #[export_name = "testing:sleep-workflow/workflow#sleep-host-activity"]
          #[allow(non_snake_case)]
          unsafe extern "C" fn __export_sleep_host_activity(arg0: i64,) {
            #[allow(unused_imports)]
            use wit_bindgen::rt::{alloc, vec::Vec, string::String};
            
            // Before executing any other code, use this function to run all static
            // constructors, if they have not yet been run. This is a hack required
            // to work around wasi-libc ctors calling import functions to initialize
            // the environment.
            //
            // This functionality will be removed once rust 1.69.0 is stable, at which
            // point wasi-libc will no longer have this behavior.
            //
            // See
            // https://github.com/bytecodealliance/preview2-prototyping/issues/99
            // for more details.
            #[cfg(target_arch="wasm32")]
            wit_bindgen::rt::run_ctors_once();
            
            <_GuestImpl as Guest>::sleep_host_activity(arg0 as u64);
          }
        };
        const _: () = {
          
          #[doc(hidden)]
          #[export_name = "testing:sleep-workflow/workflow#sleep-activity"]
          #[allow(non_snake_case)]
          unsafe extern "C" fn __export_sleep_activity(arg0: i64,) {
            #[allow(unused_imports)]
            use wit_bindgen::rt::{alloc, vec::Vec, string::String};
            
            // Before executing any other code, use this function to run all static
            // constructors, if they have not yet been run. This is a hack required
            // to work around wasi-libc ctors calling import functions to initialize
            // the environment.
            //
            // This functionality will be removed once rust 1.69.0 is stable, at which
            // point wasi-libc will no longer have this behavior.
            //
            // See
            // https://github.com/bytecodealliance/preview2-prototyping/issues/99
            // for more details.
            #[cfg(target_arch="wasm32")]
            wit_bindgen::rt::run_ctors_once();
            
            <_GuestImpl as Guest>::sleep_activity(arg0 as u64);
          }
        };
        const _: () = {
          
          #[doc(hidden)]
          #[export_name = "testing:sleep-workflow/workflow#run"]
          #[allow(non_snake_case)]
          unsafe extern "C" fn __export_run() {
            #[allow(unused_imports)]
            use wit_bindgen::rt::{alloc, vec::Vec, string::String};
            
            // Before executing any other code, use this function to run all static
            // constructors, if they have not yet been run. This is a hack required
            // to work around wasi-libc ctors calling import functions to initialize
            // the environment.
            //
            // This functionality will be removed once rust 1.69.0 is stable, at which
            // point wasi-libc will no longer have this behavior.
            //
            // See
            // https://github.com/bytecodealliance/preview2-prototyping/issues/99
            // for more details.
            #[cfg(target_arch="wasm32")]
            wit_bindgen::rt::run_ctors_once();
            
            <_GuestImpl as Guest>::run();
          }
        };
        use super::super::super::super::super::Component as _GuestImpl;
        pub trait Guest {
          fn sleep_host_activity(millis: u64,);
          fn sleep_activity(millis: u64,);
          fn run();
        }
        
      }
      
    }
  }
}

#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:any"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 568] = [3, 0, 3, 97, 110, 121, 0, 97, 115, 109, 13, 0, 1, 0, 7, 111, 1, 65, 2, 1, 66, 5, 1, 64, 1, 6, 109, 105, 108, 108, 105, 115, 119, 1, 0, 4, 0, 19, 115, 108, 101, 101, 112, 45, 104, 111, 115, 116, 45, 97, 99, 116, 105, 118, 105, 116, 121, 1, 0, 4, 0, 14, 115, 108, 101, 101, 112, 45, 97, 99, 116, 105, 118, 105, 116, 121, 1, 0, 1, 64, 0, 1, 0, 4, 0, 3, 114, 117, 110, 1, 1, 4, 1, 31, 116, 101, 115, 116, 105, 110, 103, 58, 115, 108, 101, 101, 112, 45, 119, 111, 114, 107, 102, 108, 111, 119, 47, 119, 111, 114, 107, 102, 108, 111, 119, 5, 0, 11, 14, 1, 0, 8, 119, 111, 114, 107, 102, 108, 111, 119, 3, 0, 0, 7, 193, 2, 1, 65, 2, 1, 65, 8, 1, 66, 4, 1, 64, 1, 6, 109, 105, 108, 108, 105, 115, 119, 1, 0, 4, 0, 5, 115, 108, 101, 101, 112, 1, 0, 1, 64, 0, 1, 0, 4, 0, 4, 110, 111, 111, 112, 1, 1, 3, 1, 38, 109, 121, 45, 111, 114, 103, 58, 119, 111, 114, 107, 102, 108, 111, 119, 45, 101, 110, 103, 105, 110, 101, 47, 104, 111, 115, 116, 45, 97, 99, 116, 105, 118, 105, 116, 105, 101, 115, 5, 0, 1, 66, 2, 1, 64, 1, 6, 109, 105, 108, 108, 105, 115, 119, 1, 0, 4, 0, 5, 115, 108, 101, 101, 112, 1, 0, 3, 1, 19, 116, 101, 115, 116, 105, 110, 103, 58, 115, 108, 101, 101, 112, 47, 115, 108, 101, 101, 112, 5, 1, 1, 66, 3, 1, 106, 0, 0, 1, 64, 0, 0, 0, 4, 0, 3, 114, 117, 110, 1, 1, 3, 1, 18, 119, 97, 115, 105, 58, 99, 108, 105, 47, 114, 117, 110, 64, 48, 46, 50, 46, 48, 5, 2, 1, 66, 5, 1, 64, 1, 6, 109, 105, 108, 108, 105, 115, 119, 1, 0, 4, 0, 19, 115, 108, 101, 101, 112, 45, 104, 111, 115, 116, 45, 97, 99, 116, 105, 118, 105, 116, 121, 1, 0, 4, 0, 14, 115, 108, 101, 101, 112, 45, 97, 99, 116, 105, 118, 105, 116, 121, 1, 0, 1, 64, 0, 1, 0, 4, 0, 3, 114, 117, 110, 1, 1, 4, 1, 31, 116, 101, 115, 116, 105, 110, 103, 58, 115, 108, 101, 101, 112, 45, 119, 111, 114, 107, 102, 108, 111, 119, 47, 119, 111, 114, 107, 102, 108, 111, 119, 5, 3, 4, 1, 26, 116, 101, 115, 116, 105, 110, 103, 58, 115, 108, 101, 101, 112, 45, 119, 111, 114, 107, 102, 108, 111, 119, 47, 97, 110, 121, 4, 0, 11, 9, 1, 0, 3, 97, 110, 121, 3, 2, 0, 0, 16, 12, 112, 97, 99, 107, 97, 103, 101, 45, 100, 111, 99, 115, 0, 123, 125, 0, 70, 9, 112, 114, 111, 100, 117, 99, 101, 114, 115, 1, 12, 112, 114, 111, 99, 101, 115, 115, 101, 100, 45, 98, 121, 2, 13, 119, 105, 116, 45, 99, 111, 109, 112, 111, 110, 101, 110, 116, 6, 48, 46, 49, 56, 46, 50, 16, 119, 105, 116, 45, 98, 105, 110, 100, 103, 101, 110, 45, 114, 117, 115, 116, 6, 48, 46, 49, 54, 46, 48];

#[inline(never)]
#[doc(hidden)]
#[cfg(target_arch = "wasm32")]
pub fn __link_section() {}
