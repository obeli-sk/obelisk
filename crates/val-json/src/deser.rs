use serde::de::{DeserializeSeed, Deserializer, Expected, SeqAccess, Visitor};
use std::{fmt, marker::PhantomData};

use crate::{TypeWrapper, ValWrapper};

struct ValDeserialize<'a>(&'a TypeWrapper);

impl<'a, 'de> DeserializeSeed<'de> for ValDeserialize<'a> {
    type Value = ValWrapper;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Unexpected};

        struct ExtendVecVisitor<'a>(&'a TypeWrapper);

        impl<'de, 'a> Visitor<'de> for ExtendVecVisitor<'a> {
            type Value = ValWrapper;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "value matching {type:?}", type = self.0)
            }

            fn visit_bool<E>(self, val: bool) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::Bool) {
                    Ok(ValWrapper::Bool(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Bool(val), &self))
                }
            }

            fn visit_u8<E>(self, val: u8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::U8) {
                    Ok(ValWrapper::U8(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Unsigned(val as u64), &self))
                }
            }

            fn visit_u16<E>(self, val: u16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::U16) {
                    Ok(ValWrapper::U16(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Unsigned(val as u64), &self))
                }
            }

            fn visit_u32<E>(self, val: u32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::U32) {
                    Ok(ValWrapper::U32(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Unsigned(val as u64), &self))
                }
            }

            fn visit_u64<E>(self, val: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if *self.0 == TypeWrapper::U64 {
                    Ok(ValWrapper::U64(val))
                } else if *self.0 == TypeWrapper::U8 {
                    u8::try_from(val).map(ValWrapper::U8).map_err(|_| ())
                } else if *self.0 == TypeWrapper::U16 {
                    u16::try_from(val).map(ValWrapper::U16).map_err(|_| ())
                } else if *self.0 == TypeWrapper::U32 {
                    u32::try_from(val).map(ValWrapper::U32).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S8 {
                    i8::try_from(val).map(ValWrapper::S8).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S16 {
                    i16::try_from(val).map(ValWrapper::S16).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S32 {
                    i32::try_from(val).map(ValWrapper::S32).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S64 {
                    i64::try_from(val).map(ValWrapper::S64).map_err(|_| ())
                } else {
                    Err(())
                }
                .map_err(|_| Error::invalid_type(Unexpected::Unsigned(val), &self))
            }

            fn visit_i8<E>(self, val: i8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::S8) {
                    Ok(ValWrapper::S8(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Signed(val as i64), &self))
                }
            }

            fn visit_i16<E>(self, val: i16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::U16) {
                    Ok(ValWrapper::S16(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Signed(val as i64), &self))
                }
            }

            fn visit_i32<E>(self, val: i32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::U32) {
                    Ok(ValWrapper::S32(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Signed(val as i64), &self))
                }
            }

            fn visit_i64<E>(self, val: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if *self.0 == TypeWrapper::S64 {
                    Ok(ValWrapper::S64(val))
                } else if *self.0 == TypeWrapper::U8 {
                    u8::try_from(val).map(ValWrapper::U8).map_err(|_| ())
                } else if *self.0 == TypeWrapper::U16 {
                    u16::try_from(val).map(ValWrapper::U16).map_err(|_| ())
                } else if *self.0 == TypeWrapper::U32 {
                    u32::try_from(val).map(ValWrapper::U32).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S8 {
                    i8::try_from(val).map(ValWrapper::S8).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S16 {
                    i16::try_from(val).map(ValWrapper::S16).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S32 {
                    i32::try_from(val).map(ValWrapper::S32).map_err(|_| ())
                } else if *self.0 == TypeWrapper::U64 {
                    u64::try_from(val).map(ValWrapper::U64).map_err(|_| ())
                } else {
                    Err(())
                }
                .map_err(|_| Error::invalid_type(Unexpected::Signed(val), &self))
            }

            fn visit_f32<E>(self, val: f32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::Float32) {
                    Ok(ValWrapper::Float32(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Float(val as f64), &self))
                }
            }

            fn visit_f64<E>(self, val: f64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if *self.0 == TypeWrapper::Float64 {
                    Ok(ValWrapper::Float64(val))
                } else if *self.0 == TypeWrapper::Float32 {
                    let f32 = val as f32;
                    if val.is_finite() == f32.is_finite() {
                        Ok(ValWrapper::Float32(f32))
                    } else {
                        Err(())
                    }
                } else {
                    Err(())
                }
                .map_err(|_| Error::invalid_type(Unexpected::Float(val), &self))
            }

            fn visit_char<E>(self, val: char) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::Char) {
                    Ok(ValWrapper::Char(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Char(val), &self))
                }
            }

            fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::String) {
                    Ok(ValWrapper::String(val.into()))
                } else {
                    Err(Error::invalid_type(Unexpected::Str(val), &self))
                }
            }
        }

        deserializer.deserialize_any(ExtendVecVisitor(self.0))
    }
}

// Visitor implementation that deserializes a JSON array into `Vec<V: From<ValWrapper>>`.
struct SequenceVisitor<'a, V>(&'a [TypeWrapper], PhantomData<V>);

impl<'a, 'de, V: From<ValWrapper>> Visitor<'de> for SequenceVisitor<'a, V> {
    type Value = Vec<V>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "an array of length {}", self.0.len())
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        use serde::de::Error;
        let mut vec = Vec::new();
        for (idx, ty) in self.0.iter().enumerate() {
            if let Some(val) = seq.next_element_seed(ValDeserialize(ty))? {
                vec.push(V::from(val));
            } else {
                Err(Error::invalid_length(idx, &self))?;
            }
        }
        // Check if there are more values available than the number of types provided.
        if let Ok(Some(unexpected)) = seq.next_element::<serde_json::Value>() {
            Err(Error::custom(format_args!(
                "invalid length that is too big, at element `{unexpected:?}`, expected {}",
                &self as &dyn Expected
            )))?;
        }
        Ok(vec)
    }
}

pub fn deserialize_sequence<V: From<ValWrapper>>(
    params: &str,
    param_types: &[TypeWrapper],
) -> Result<Vec<V>, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_str(params);
    let visitor = SequenceVisitor(param_types, PhantomData);
    deserializer.deserialize_seq(visitor)
}

#[cfg(test)]
mod tests {
    use crate::{deser::ValDeserialize, TypeWrapper, ValWrapper};
    use serde::de::DeserializeSeed;
    use wasmtime::component::Val;

    #[test]
    fn bool() {
        let expected = ValWrapper::Bool(true);
        let input = "true";
        let ty = TypeWrapper::Bool;
        let actual = ValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn string() {
        let expected = ValWrapper::String("test".into());
        let input = r#""test""#;
        let ty = TypeWrapper::String;
        let actual = ValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn u8() {
        let expected = ValWrapper::U8(123);
        let input = "123";
        let ty = TypeWrapper::U8;
        let actual = ValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn s8() {
        let expected = ValWrapper::S8(-123);
        let input = "-123";
        let ty = TypeWrapper::S8;
        let actual = ValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn f32() {
        let expected = ValWrapper::Float32(-123.1);
        let input = "-123.1";
        let ty = TypeWrapper::Float32;
        let actual = ValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn f32_max() {
        let expected = ValWrapper::Float32(f32::MAX);
        let input = f32::MAX.to_string();
        let ty = TypeWrapper::Float32;
        let actual = ValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(&input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn f64() {
        let f = f32::MAX as f64 + 1.0;
        let expected = ValWrapper::Float64(f);
        let input = f.to_string();
        let ty = TypeWrapper::Float64;
        let actual = ValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(&input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn f32_from_f64_overflow1() {
        let f = f32::MAX as f64 * 2.0;
        let input = f.to_string();
        let ty = TypeWrapper::Float32;
        let err = ValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(&input))
            .unwrap_err();
        assert_starts_with(&err,
            "invalid type: floating point `680564693277057700000000000000000000000`, expected value matching Float32");
    }

    #[test]
    fn f64_out_of_range() {
        let input = f64::MAX.to_string();
        let ty = TypeWrapper::Float64;
        let err = ValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(&input))
            .unwrap_err();
        assert_starts_with(&err, "number out of range");
    }

    #[test]
    fn deserialize_sequence() {
        let expected = vec![
            ValWrapper::Bool(true),
            ValWrapper::U8(8),
            ValWrapper::S16(-16),
        ];
        let param_types = r#"["Bool", "U8", "S16"]"#;
        let param_vals = "[true, 8, -16]";
        let param_types: Vec<TypeWrapper> = serde_json::from_str(param_types).unwrap();
        let actual = crate::deserialize_sequence(param_vals, &param_types).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn deserialize_sequence_too_many_types() {
        let param_types = r#"["Bool", "U8", "S16"]"#;
        let param_vals = "[true, 8]";
        let param_types: Vec<TypeWrapper> = serde_json::from_str(param_types).unwrap();
        let err = crate::deserialize_sequence::<Val>(param_vals, &param_types).unwrap_err();
        assert_starts_with(&err, "invalid length 2, expected an array of length 3");
    }

    #[test]
    fn deserialize_sequence_too_many_values() {
        let param_types = r#"["Bool", "U8"]"#;
        let param_vals = "[true, 8, false]";
        let param_types: Vec<TypeWrapper> = serde_json::from_str(param_types).unwrap();
        let err = crate::deserialize_sequence::<Val>(param_vals, &param_types).unwrap_err();
        assert_starts_with(
            &err,
            "invalid length that is too big, at element `Bool(false)`, expected an array of length 2",
        );
    }

    fn assert_starts_with(err: &dyn std::error::Error, starts_with: &str) {
        let actual = err.to_string();
        assert!(
            actual.starts_with(starts_with),
            "Unexpected error\nGot:\n{actual}\nExpected to start with:\n{starts_with}"
        );
    }
}
