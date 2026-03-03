//! Obelisk environment variable access for JS runtimes.
//!
//! Provides `obelisk.env(key)` function to read environment variables.
//!
//! # JS API
//!
//! ```js
//! // Get environment variable value (returns string or undefined)
//! const value = obelisk.env("MY_VAR");
//! if (value !== undefined) {
//!     console.log("MY_VAR =", value);
//! }
//! ```

use boa_engine::{
    Context, JsArgs, JsNativeError, JsObject, JsResult, JsValue, NativeFunction, js_string,
};

/// Register `obelisk.env` function on the given obelisk object.
///
/// # Arguments
/// * `obelisk` - The obelisk global object to add the `env` function to
/// * `context` - The Boa JS context
pub fn register_env(obelisk: &JsObject, context: &mut Context) -> JsResult<()> {
    let env_fn = NativeFunction::from_fn_ptr(|_this, args, _ctx| {
        let key = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("key must be a string"))?
            .to_std_string_escaped();

        match std::env::var(&key) {
            Ok(value) => Ok(JsValue::from(js_string!(value))),
            Err(_) => Ok(JsValue::undefined()),
        }
    });

    obelisk.set(
        js_string!("env"),
        env_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::new_object;
    use boa_engine::property::Attribute;

    #[test]
    fn test_env_returns_value_when_set() {
        // SAFETY: Test runs in a single thread and uses a unique env var name
        unsafe {
            std::env::set_var("OBELISK_TEST_VAR", "test_value");
        }

        let mut context = Context::default();
        let obelisk = new_object(&mut context);
        register_env(&obelisk, &mut context).expect("register_env should succeed");
        context
            .register_global_property(js_string!("obelisk"), obelisk, Attribute::all())
            .expect("register obelisk global");

        let result = context
            .eval(boa_engine::Source::from_bytes(
                r#"obelisk.env("OBELISK_TEST_VAR")"#,
            ))
            .expect("eval should succeed");

        assert_eq!(
            result.as_string().unwrap().to_std_string_escaped(),
            "test_value"
        );

        // SAFETY: Test cleanup
        unsafe {
            std::env::remove_var("OBELISK_TEST_VAR");
        }
    }

    #[test]
    fn test_env_returns_undefined_when_not_set() {
        // SAFETY: Test runs in a single thread and uses a unique env var name
        unsafe {
            std::env::remove_var("OBELISK_NONEXISTENT_VAR");
        }

        let mut context = Context::default();
        let obelisk = new_object(&mut context);
        register_env(&obelisk, &mut context).expect("register_env should succeed");
        context
            .register_global_property(js_string!("obelisk"), obelisk, Attribute::all())
            .expect("register obelisk global");

        let result = context
            .eval(boa_engine::Source::from_bytes(
                r#"obelisk.env("OBELISK_NONEXISTENT_VAR")"#,
            ))
            .expect("eval should succeed");

        assert!(result.is_undefined());
    }

    #[test]
    fn test_env_throws_on_non_string_key() {
        let mut context = Context::default();
        let obelisk = new_object(&mut context);
        register_env(&obelisk, &mut context).expect("register_env should succeed");
        context
            .register_global_property(js_string!("obelisk"), obelisk, Attribute::all())
            .expect("register obelisk global");

        let result = context.eval(boa_engine::Source::from_bytes(r#"obelisk.env(123)"#));

        assert!(result.is_err());
    }
}
