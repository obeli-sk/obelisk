#![allow(dead_code)]
// Generated by `wit-bindgen` 0.16.0. DO NOT EDIT!
pub mod testing {
  pub mod http {
    
    #[allow(clippy::all)]
    pub mod http_get {
      #[used]
      #[doc(hidden)]
      #[cfg(target_arch = "wasm32")]
      static __FORCE_SECTION_REF: fn() = super::super::super::__link_section;
      #[allow(unused_unsafe, clippy::all)]
      pub fn get(authority: &str,path: &str,) -> Result<wit_bindgen::rt::string::String,wit_bindgen::rt::string::String>{
        
        #[allow(unused_imports)]
        use wit_bindgen::rt::{alloc, vec::Vec, string::String};
        unsafe {
          
          #[repr(align(4))]
          struct RetArea([u8; 12]);
          let mut ret_area = ::core::mem::MaybeUninit::<RetArea>::uninit();
          let vec0 = authority;
          let ptr0 = vec0.as_ptr() as i32;
          let len0 = vec0.len() as i32;
          let vec1 = path;
          let ptr1 = vec1.as_ptr() as i32;
          let len1 = vec1.len() as i32;
          let ptr2 = ret_area.as_mut_ptr() as i32;
          #[cfg(target_arch = "wasm32")]
          #[link(wasm_import_module = "testing:http/http-get")]
          extern "C" {
            #[link_name = "get"]
            fn wit_import(_: i32, _: i32, _: i32, _: i32, _: i32, );
          }
          
          #[cfg(not(target_arch = "wasm32"))]
          fn wit_import(_: i32, _: i32, _: i32, _: i32, _: i32, ){ unreachable!() }
          wit_import(ptr0, len0, ptr1, len1, ptr2);
          let l3 = i32::from(*((ptr2 + 0) as *const u8));
          match l3 {
            0 => {
              let e = {
                let l4 = *((ptr2 + 4) as *const i32);
                let l5 = *((ptr2 + 8) as *const i32);
                let len6 = l5 as usize;
                let bytes6 = Vec::from_raw_parts(l4 as *mut _, len6, len6);
                
                wit_bindgen::rt::string_lift(bytes6)
              };
              Ok(e)
            }
            1 => {
              let e = {
                let l7 = *((ptr2 + 4) as *const i32);
                let l8 = *((ptr2 + 8) as *const i32);
                let len9 = l8 as usize;
                let bytes9 = Vec::from_raw_parts(l7 as *mut _, len9, len9);
                
                wit_bindgen::rt::string_lift(bytes9)
              };
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
    pub mod http_workflow {
      
      #[allow(clippy::all)]
      pub mod workflow {
        #[used]
        #[doc(hidden)]
        #[cfg(target_arch = "wasm32")]
        static __FORCE_SECTION_REF: fn() = super::super::super::super::__link_section;
        const _: () = {
          
          #[doc(hidden)]
          #[export_name = "testing:http-workflow/workflow#execute"]
          #[allow(non_snake_case)]
          unsafe extern "C" fn __export_execute(arg0: i32,) -> i32 {
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
            
            let result0 = <_GuestImpl as Guest>::execute(arg0 as u16);
            let ptr1 = _RET_AREA.0.as_mut_ptr() as i32;
            let vec2 = (result0.into_bytes()).into_boxed_slice();
            let ptr2 = vec2.as_ptr() as i32;
            let len2 = vec2.len() as i32;
            ::core::mem::forget(vec2);
            *((ptr1 + 4) as *mut i32) = len2;
            *((ptr1 + 0) as *mut i32) = ptr2;
            ptr1
          }
          
          const _: () = {
            #[doc(hidden)]
            #[export_name = "cabi_post_testing:http-workflow/workflow#execute"]
            #[allow(non_snake_case)]
            unsafe extern "C" fn __post_return_execute(arg0: i32,) {
              let l0 = *((arg0 + 0) as *const i32);
              let l1 = *((arg0 + 4) as *const i32);
              wit_bindgen::rt::dealloc(l0, (l1) as usize, 1);
            }
          };
        };
        use super::super::super::super::super::Component as _GuestImpl;
        pub trait Guest {
          fn execute(port: u16,) -> wit_bindgen::rt::string::String;
        }
        
        #[allow(unused_imports)]
        use wit_bindgen::rt::{alloc, vec::Vec, string::String};
        
        #[repr(align(4))]
        struct _RetArea([u8; 8]);
        static mut _RET_AREA: _RetArea = _RetArea([0; 8]);
        
      }
      
    }
  }
}

#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:any"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 362] = [3, 0, 3, 97, 110, 121, 0, 97, 115, 109, 13, 0, 1, 0, 7, 64, 1, 65, 2, 1, 66, 2, 1, 64, 1, 4, 112, 111, 114, 116, 123, 0, 115, 4, 0, 7, 101, 120, 101, 99, 117, 116, 101, 1, 0, 4, 1, 30, 116, 101, 115, 116, 105, 110, 103, 58, 104, 116, 116, 112, 45, 119, 111, 114, 107, 102, 108, 111, 119, 47, 119, 111, 114, 107, 102, 108, 111, 119, 5, 0, 11, 14, 1, 0, 8, 119, 111, 114, 107, 102, 108, 111, 119, 3, 0, 0, 7, 162, 1, 1, 65, 2, 1, 65, 4, 1, 66, 3, 1, 106, 1, 115, 1, 115, 1, 64, 2, 9, 97, 117, 116, 104, 111, 114, 105, 116, 121, 115, 4, 112, 97, 116, 104, 115, 0, 0, 4, 0, 3, 103, 101, 116, 1, 1, 3, 1, 21, 116, 101, 115, 116, 105, 110, 103, 58, 104, 116, 116, 112, 47, 104, 116, 116, 112, 45, 103, 101, 116, 5, 0, 1, 66, 2, 1, 64, 1, 4, 112, 111, 114, 116, 123, 0, 115, 4, 0, 7, 101, 120, 101, 99, 117, 116, 101, 1, 0, 4, 1, 30, 116, 101, 115, 116, 105, 110, 103, 58, 104, 116, 116, 112, 45, 119, 111, 114, 107, 102, 108, 111, 119, 47, 119, 111, 114, 107, 102, 108, 111, 119, 5, 1, 4, 1, 25, 116, 101, 115, 116, 105, 110, 103, 58, 104, 116, 116, 112, 45, 119, 111, 114, 107, 102, 108, 111, 119, 47, 97, 110, 121, 4, 0, 11, 9, 1, 0, 3, 97, 110, 121, 3, 2, 0, 0, 16, 12, 112, 97, 99, 107, 97, 103, 101, 45, 100, 111, 99, 115, 0, 123, 125, 0, 70, 9, 112, 114, 111, 100, 117, 99, 101, 114, 115, 1, 12, 112, 114, 111, 99, 101, 115, 115, 101, 100, 45, 98, 121, 2, 13, 119, 105, 116, 45, 99, 111, 109, 112, 111, 110, 101, 110, 116, 6, 48, 46, 49, 56, 46, 50, 16, 119, 105, 116, 45, 98, 105, 110, 100, 103, 101, 110, 45, 114, 117, 115, 116, 6, 48, 46, 49, 54, 46, 48];

#[inline(never)]
#[doc(hidden)]
#[cfg(target_arch = "wasm32")]
pub fn __link_section() {}
