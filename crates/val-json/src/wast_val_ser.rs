use crate::{
    type_wrapper::{TypeKey, TypeWrapper},
    wast_val::{ValKey, WastVal, WastValWithType},
};
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{
    Deserialize, Serialize, Serializer,
    de::{self, DeserializeSeed, Deserializer, MapAccess, Visitor},
};

// Lossless conversions between integer and floating-point representations.
//
// `std` provides `From` / `TryFrom` only for integer ↔ integer and a handful of
// small-int → float pairs (e.g. `f64::from(u32)`). For u64/i64 ↔ f32/f64 and
// f32/f64 → int there is no checked conversion in std, so we use `as`. The
// helpers below confine those casts in one documented place and verify
// losslessness via 128-bit round-trip (the 128-bit detour avoids the
// saturation false-positive at u64::MAX / i64::MAX, where
// `(int as float) as int` saturates back to a value that incidentally equals
// the input).

#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn u64_to_f64_lossless(val: u64) -> Option<f64> {
    let f = val as f64;
    let round_trip = f as u128;
    (round_trip == u128::from(val)).then_some(f)
}

#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn u64_to_f32_lossless(val: u64) -> Option<f32> {
    let f = val as f32;
    if !f.is_finite() {
        return None;
    }
    let round_trip = f as u128;
    (round_trip == u128::from(val)).then_some(f)
}

#[expect(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn i64_to_f64_lossless(val: i64) -> Option<f64> {
    let f = val as f64;
    let round_trip = f as i128;
    (round_trip == i128::from(val)).then_some(f)
}

#[expect(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn i64_to_f32_lossless(val: i64) -> Option<f32> {
    let f = val as f32;
    if !f.is_finite() {
        return None;
    }
    let round_trip = f as i128;
    (round_trip == i128::from(val)).then_some(f)
}

const JS_MAX_SAFE_INTEGER_F64: f64 = 9_007_199_254_740_991.0;

// f64 -> integer: returns the integer value iff `val` is finite, whole-number,
// non-negative (for u64) / negative (for i64), and fits losslessly in the
// target integer. serde_json has already rounded the source decimal by the time
// it calls `visit_f64`, so values outside JS's safe integer range are rejected
// even if the rounded f64 itself happens to be exactly integral.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::float_cmp
)]
fn f64_to_u64_exact(val: f64) -> Option<u64> {
    if !val.is_finite() || !(0.0..=JS_MAX_SAFE_INTEGER_F64).contains(&val) || val != val.trunc() {
        return None;
    }
    let big = val as u128;
    u64::try_from(big).ok()
}

#[expect(clippy::cast_possible_truncation, clippy::float_cmp)]
fn f64_to_i64_exact(val: f64) -> Option<i64> {
    if !val.is_finite() || !(-JS_MAX_SAFE_INTEGER_F64..0.0).contains(&val) || val != val.trunc() {
        return None;
    }
    let big = val as i128;
    i64::try_from(big).ok()
}

impl Serialize for WastVal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            WastVal::Bool(v) => serializer.serialize_bool(*v),
            WastVal::S8(v) => serializer.serialize_i8(*v),
            WastVal::U8(v) => serializer.serialize_u8(*v),
            WastVal::S16(v) => serializer.serialize_i16(*v),
            WastVal::U16(v) => serializer.serialize_u16(*v),
            WastVal::S32(v) => serializer.serialize_i32(*v),
            WastVal::U32(v) => serializer.serialize_u32(*v),
            WastVal::S64(v) => serializer.serialize_i64(*v),
            WastVal::U64(v) => serializer.serialize_u64(*v),
            WastVal::F32(v) => serializer.serialize_f32(*v),
            WastVal::F64(v) => serializer.serialize_f64(*v),
            WastVal::Char(v) => serializer.serialize_char(*v),
            WastVal::String(v) => serializer.serialize_str(v),
            WastVal::List(list) | WastVal::Tuple(list) => {
                use serde::ser::SerializeSeq;
                let mut seq = serializer.serialize_seq(Some(list.len()))?;
                for val in list {
                    seq.serialize_element(val)?;
                }
                seq.end()
            }
            WastVal::Record(record) => {
                use serde::ser::SerializeMap;
                let mut ser = serializer.serialize_map(Some(record.len()))?;
                for (key, value) in record {
                    ser.serialize_entry(key, value)?;
                }
                ser.end()
            }
            WastVal::Enum(key) | WastVal::Variant(key, None) => {
                serializer.serialize_str(key.as_snake_str())
            }
            WastVal::Variant(key, Some(value)) => {
                use serde::ser::SerializeMap;
                let mut ser = serializer.serialize_map(Some(1))?;
                ser.serialize_entry(key, value)?;
                ser.end()
            }
            WastVal::Option(option) => {
                if let Some(some) = option {
                    serializer.serialize_some(some)
                } else {
                    serializer.serialize_none()
                }
            }
            WastVal::Result(result) => match result {
                Ok(ok) => serializer.serialize_newtype_variant("Result", 0, "ok", ok),
                Err(err) => serializer.serialize_newtype_variant("Result", 0, "err", err),
            },
            WastVal::Flags(flags) => {
                use serde::ser::SerializeSeq;
                let mut seq = serializer.serialize_seq(Some(flags.len()))?;
                for val in flags {
                    seq.serialize_element(val)?;
                }
                seq.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for WastValWithType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["type", "value"];
        deserializer.deserialize_struct("WastValWithType", FIELDS, WastValWithTypeVisitor)
    }
}

struct WastValWithTypeVisitor;

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum WastValWithTypeField {
    Type,
    Value,
}

impl<'de> Visitor<'de> for WastValWithTypeVisitor {
    type Value = WastValWithType;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("struct WastValWithType")
    }

    fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        let mut r#type = None;
        let mut value = None;
        while let Some(key) = map.next_key()? {
            match key {
                WastValWithTypeField::Type => {
                    if r#type.is_some() {
                        return Err(de::Error::duplicate_field("type"));
                    }
                    r#type = Some(map.next_value()?);
                }
                WastValWithTypeField::Value => {
                    if value.is_some() {
                        return Err(de::Error::duplicate_field("value"));
                    }
                    if let Some(ty) = &r#type {
                        value = Some(map.next_value_seed(WastValDeserialize(ty))?);
                    } else {
                        return Err(de::Error::custom("`type` field must preceed `value` field"));
                    }
                }
            }
        }
        let r#type = r#type.ok_or_else(|| de::Error::missing_field("type"))?;
        let value = value.ok_or_else(|| de::Error::missing_field("value"))?;
        Ok(WastValWithType { r#type, value })
    }
}

struct WastValDeserialize<'a>(&'a TypeWrapper);

impl<'de> DeserializeSeed<'de> for WastValDeserialize<'_> {
    type Value = WastVal;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Unexpected};

        struct WastValVisitor<'a>(&'a TypeWrapper);

        impl<'de> Visitor<'de> for WastValVisitor<'_> {
            type Value = WastVal;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                let type_wrapper = serde_json::to_string(&self.0)
                    .expect("all type wrappers must be JSON serializable, no NaNs or custom ser");
                write!(formatter, "value matching {type_wrapper}") //  WIT type cannot be displayed without names
            }

            fn visit_bool<E>(self, val: bool) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::Bool) {
                    Ok(WastVal::Bool(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Bool(val), &self))
                }
            }

            fn visit_u8<E>(self, val: u8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::U8) {
                    Ok(WastVal::U8(val))
                } else {
                    Err(Error::invalid_type(
                        Unexpected::Unsigned(u64::from(val)),
                        &self,
                    ))
                }
            }

            fn visit_u16<E>(self, val: u16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::U16) {
                    Ok(WastVal::U16(val))
                } else {
                    Err(Error::invalid_type(
                        Unexpected::Unsigned(u64::from(val)),
                        &self,
                    ))
                }
            }

            fn visit_u32<E>(self, val: u32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::U32) {
                    Ok(WastVal::U32(val))
                } else {
                    Err(Error::invalid_type(
                        Unexpected::Unsigned(u64::from(val)),
                        &self,
                    ))
                }
            }

            fn visit_u64<E>(self, val: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if *self.0 == TypeWrapper::U64 {
                    Ok(WastVal::U64(val))
                } else if *self.0 == TypeWrapper::U8 {
                    u8::try_from(val).map(WastVal::U8).map_err(|_| ())
                } else if *self.0 == TypeWrapper::U16 {
                    u16::try_from(val).map(WastVal::U16).map_err(|_| ())
                } else if *self.0 == TypeWrapper::U32 {
                    u32::try_from(val).map(WastVal::U32).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S8 {
                    i8::try_from(val).map(WastVal::S8).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S16 {
                    i16::try_from(val).map(WastVal::S16).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S32 {
                    i32::try_from(val).map(WastVal::S32).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S64 {
                    i64::try_from(val).map(WastVal::S64).map_err(|_| ())
                } else if *self.0 == TypeWrapper::F64 {
                    // Accept ints for f64 only when the cast is lossless. Boa's
                    // `to_json` sometimes tags whole-number Numbers as int32
                    // even when the WIT field is float; reject when the f64
                    // can't represent `val` exactly (val > 2^53 with non-zero
                    // trailing bits).
                    u64_to_f64_lossless(val).map(WastVal::F64).ok_or(())
                } else if *self.0 == TypeWrapper::F32 {
                    u64_to_f32_lossless(val).map(WastVal::F32).ok_or(())
                } else {
                    Err(())
                }
                .map_err(|()| Error::invalid_type(Unexpected::Unsigned(val), &self))
            }

            fn visit_i8<E>(self, val: i8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::S8) {
                    Ok(WastVal::S8(val))
                } else {
                    Err(Error::invalid_type(
                        Unexpected::Signed(i64::from(val)),
                        &self,
                    ))
                }
            }

            fn visit_i16<E>(self, val: i16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::U16) {
                    Ok(WastVal::S16(val))
                } else {
                    Err(Error::invalid_type(
                        Unexpected::Signed(i64::from(val)),
                        &self,
                    ))
                }
            }

            fn visit_i32<E>(self, val: i32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::U32) {
                    Ok(WastVal::S32(val))
                } else {
                    Err(Error::invalid_type(
                        Unexpected::Signed(i64::from(val)),
                        &self,
                    ))
                }
            }

            fn visit_i64<E>(self, val: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if *self.0 == TypeWrapper::S64 {
                    Ok(WastVal::S64(val))
                } else if *self.0 == TypeWrapper::U8 {
                    u8::try_from(val).map(WastVal::U8).map_err(|_| ())
                } else if *self.0 == TypeWrapper::U16 {
                    u16::try_from(val).map(WastVal::U16).map_err(|_| ())
                } else if *self.0 == TypeWrapper::U32 {
                    u32::try_from(val).map(WastVal::U32).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S8 {
                    i8::try_from(val).map(WastVal::S8).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S16 {
                    i16::try_from(val).map(WastVal::S16).map_err(|_| ())
                } else if *self.0 == TypeWrapper::S32 {
                    i32::try_from(val).map(WastVal::S32).map_err(|_| ())
                } else if *self.0 == TypeWrapper::U64 {
                    u64::try_from(val).map(WastVal::U64).map_err(|_| ())
                } else if *self.0 == TypeWrapper::F64 {
                    i64_to_f64_lossless(val).map(WastVal::F64).ok_or(())
                } else if *self.0 == TypeWrapper::F32 {
                    i64_to_f32_lossless(val).map(WastVal::F32).ok_or(())
                } else {
                    Err(())
                }
                .map_err(|()| Error::invalid_type(Unexpected::Signed(val), &self))
            }

            fn visit_f32<E>(self, val: f32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::F32) {
                    Ok(WastVal::F32(val))
                } else {
                    Err(Error::invalid_type(
                        Unexpected::Float(f64::from(val)),
                        &self,
                    ))
                }
            }

            fn visit_f64<E>(self, val: f64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if *self.0 == TypeWrapper::F64 {
                    return Ok(WastVal::F64(val));
                }
                if *self.0 == TypeWrapper::F32 {
                    #[expect(clippy::cast_possible_truncation)]
                    let f32 = val as f32;
                    // Round-trip via the lossless f32 → f64 widening: `val`
                    // fits in f32 iff the widened f32 equals the original.
                    // Catches both overflow (val outside f32 range rounds to
                    // ±INFINITY ≠ val) and mantissa loss (e.g. `0.1` whose
                    // f64 and f32 bit patterns differ, or any whole number
                    // above 2^24 with bits beyond f32's 24-bit mantissa).
                    #[expect(clippy::float_cmp)]
                    if f64::from(f32) == val {
                        return Ok(WastVal::F32(f32));
                    }
                    return Err(Error::invalid_type(Unexpected::Float(val), &self));
                }

                // Accept whole-number f64 for integer WIT types. Boa's `to_json`
                // tags any whole-number JS Number outside the int32 range as f64,
                // so without this branch values like `9999999999` cannot reach
                // an s64 field from JS. Route through the existing integer
                // visitors so their per-type range checks (e.g. fitting into u8)
                // apply unchanged.
                let int_ty = matches!(
                    *self.0,
                    TypeWrapper::U8
                        | TypeWrapper::U16
                        | TypeWrapper::U32
                        | TypeWrapper::U64
                        | TypeWrapper::S8
                        | TypeWrapper::S16
                        | TypeWrapper::S32
                        | TypeWrapper::S64
                );
                if int_ty {
                    if let Some(u) = f64_to_u64_exact(val) {
                        return self.visit_u64(u);
                    }
                    if let Some(i) = f64_to_i64_exact(val) {
                        return self.visit_i64(i);
                    }
                }
                Err(Error::invalid_type(Unexpected::Float(val), &self))
            }

            fn visit_char<E>(self, val: char) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::Char) {
                    Ok(WastVal::Char(val))
                } else {
                    Err(Error::invalid_type(Unexpected::Char(val), &self))
                }
            }

            fn visit_str<E>(self, str_val: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                match self.0 {
                    TypeWrapper::String => Ok(WastVal::String(str_val.into())),
                    TypeWrapper::Variant(possible_variants) => {
                        if let Some(found) = possible_variants.get(&TypeKey::from_snake(str_val)) {
                            if found.is_none() {
                                Ok(WastVal::Variant(
                                    ValKey::new_snake(str_val.to_string()),
                                    None,
                                ))
                            } else {
                                Err(Error::custom(format!(
                                    "cannot deserialize variant: `{str_val}` must be serialized as map"
                                )))
                            }
                        } else {
                            Err(Error::custom(format!(
                                "cannot deserialize enum: `{str_val}` not found in the following list: `{:?}`",
                                possible_variants
                                    .keys()
                                    .map(TypeKey::to_snake_string) // display error in `snake_case`
                                    .collect_vec()
                            )))
                        }
                    }
                    TypeWrapper::Enum(possible_variants) => {
                        if possible_variants.contains(&TypeKey::from_snake(str_val)) {
                            Ok(WastVal::Enum(ValKey::new_snake(str_val.to_string())))
                        } else {
                            Err(Error::custom(format!(
                                "cannot deserialize enum: `{str_val}` not found in the following list: `{:?}`",
                                possible_variants
                                    .iter()
                                    .map(TypeKey::to_snake_string) // display error in `snake_case`
                                    .collect_vec()
                            )))
                        }
                    }
                    _ => Err(Error::invalid_type(Unexpected::Str(str_val), &self)),
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                match self.0 {
                    TypeWrapper::Result { ok, err } => {
                        let ok_or_err = map
                            .next_key::<String>()?
                            .ok_or(Error::custom("invalid map key, expected String"))?;
                        if ok_or_err == "ok" {
                            if let Some(ok) = ok {
                                let ok = map.next_value_seed(WastValDeserialize(ok))?;
                                Ok(WastVal::Result(Ok(Some(ok.into()))))
                            } else {
                                let val = map.next_value::<Option<()>>();
                                match val {
                                    Err(_) | Ok(Some(())) => {
                                        Err(Error::custom("invalid result value, expected null"))
                                    }
                                    Ok(None) => Ok(WastVal::Result(Ok(None))),
                                }
                            }
                        } else if ok_or_err == "err" {
                            if let Some(err) = err {
                                let err = map.next_value_seed(WastValDeserialize(err))?;
                                Ok(WastVal::Result(Err(Some(err.into()))))
                            } else {
                                let val = map.next_value::<Option<()>>();
                                match val {
                                    Err(_) | Ok(Some(())) => {
                                        Err(Error::custom("invalid result value, expected null"))
                                    }
                                    Ok(None) => Ok(WastVal::Result(Err(None))),
                                }
                            }
                        } else {
                            Err(Error::custom("expected one of `ok`, `err`"))
                        }
                    }
                    TypeWrapper::Record(record) => {
                        let mut input_map = hashbrown::HashMap::new();
                        while let Some(field_name) = map.next_key::<String>()? {
                            let type_key = TypeKey::from_snake(&field_name);
                            if let Some(field_type) = record.get(&type_key) {
                                let value = map.next_value_seed(WastValDeserialize(field_type))?;
                                input_map.insert(type_key, value);
                            } else {
                                return Err(Error::custom(format!(
                                    "unexpected record field name `{field_name}`" // display field in original `snake_case`
                                )));
                            }
                        }
                        // Ordering must be based on seed, not actual JSON input.
                        let mut dst_map = IndexMap::new();
                        let mut missing = vec![];
                        for (requested_field_name, requested_ty) in record {
                            if let Some((requested_field_name, value)) =
                                input_map.remove_entry(requested_field_name)
                            {
                                dst_map.insert(ValKey::from(&requested_field_name), value);
                            } else if matches!(requested_ty, TypeWrapper::Option(_)) {
                                dst_map.insert(
                                    ValKey::from(requested_field_name),
                                    WastVal::Option(None),
                                );
                            } else {
                                missing.push(requested_field_name.to_snake_string()); // display in `snake_case`
                            }
                        }
                        if missing.is_empty() {
                            Ok(WastVal::Record(dst_map))
                        } else {
                            Err(Error::custom(format!(
                                "cannot deserialize record: missing fields `{missing:?}`"
                            )))
                        }
                    }
                    TypeWrapper::Variant(possible_variants) => {
                        if let Some(variant_name) = map.next_key::<String>()? {
                            if let Some(found) =
                                possible_variants.get(&TypeKey::from_snake(&variant_name))
                            {
                                if let Some(variant_field_type) = found {
                                    let value = map
                                        .next_value_seed(WastValDeserialize(variant_field_type))?;
                                    Ok(WastVal::Variant(
                                        ValKey::new_snake(variant_name), // display error in `snake_case`
                                        Some(Box::new(value)),
                                    ))
                                } else {
                                    Err(Error::custom(format!(
                                        "cannot deserialize variant: `{variant_name}` must be serialized as string"
                                    )))
                                }
                            } else {
                                Err(Error::custom(format!(
                                    "cannot deserialize variant: `{variant_name}` not found in the following list: `{:?}`",
                                    possible_variants
                                        .keys()
                                        .map(TypeKey::to_snake_string) // display error in `snake_case`
                                        .collect_vec()
                                )))
                            }
                        } else {
                            Err(Error::custom(format!(
                                "cannot deserialize variant: expected single map key from following list: `{:?}`",
                                possible_variants
                                    .keys()
                                    .map(TypeKey::to_snake_string) // display error in `snake_case`
                                    .collect_vec()
                            )))
                        }
                    }
                    _ => Err(Error::invalid_type(Unexpected::Map, &self)),
                }
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                // bugfix for `join_set_deser_should_work`
                self.visit_none()
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                if let TypeWrapper::Option(inner_seed) = self.0 {
                    let inner_value = WastValDeserialize(inner_seed).deserialize(deserializer)?;
                    Ok(WastVal::Option(Some(inner_value.into())))
                } else {
                    Err(Error::invalid_type(Unexpected::Option, &self))
                }
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if matches!(self.0, TypeWrapper::Option(_)) {
                    Ok(WastVal::Option(None))
                } else {
                    Err(Error::invalid_type(Unexpected::Option, &self))
                }
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                if let TypeWrapper::List(inner_seed) = self.0 {
                    let mut vec = Vec::new();
                    while let Some(element) =
                        seq.next_element_seed(WastValDeserialize(inner_seed.as_ref()))?
                    {
                        vec.push(element);
                    }
                    Ok(WastVal::List(vec))
                } else if let TypeWrapper::Tuple(type_sequence) = self.0 {
                    let expected_size = type_sequence.len();
                    if let Some(actual_size) = seq.size_hint()
                        && actual_size != expected_size
                    {
                        return Err(Error::invalid_length(actual_size, &self));
                    }
                    let mut vec = Vec::new();
                    for (idx, seed) in type_sequence.iter().enumerate() {
                        if let Some(element) = seq.next_element_seed(WastValDeserialize(seed))? {
                            vec.push(element);
                        } else {
                            return Err(Error::invalid_length(idx, &self));
                        }
                    }
                    // No more elements are expected.
                    let unexpected: Option<serde_json::Value> = seq.next_element()?;
                    if unexpected.is_some() {
                        return Err(Error::invalid_length(expected_size + 1, &self));
                    }
                    Ok(WastVal::Tuple(vec))
                } else if let TypeWrapper::Flags(possible_flags) = self.0 {
                    let mut vec = Vec::with_capacity(possible_flags.len());
                    while let Some(element) = seq.next_element::<String>()? {
                        let element = ValKey::new_snake(element);
                        if vec.contains(&element) {
                            return Err(Error::custom(format!(
                                "cannot deserialize flags: flag `{element}` was found more than once"
                            )));
                        } else if possible_flags.contains(&TypeKey::from(&element)) {
                            vec.push(element);
                        } else {
                            return Err(Error::custom(format!(
                                "cannot deserialize flags: flag `{element}` not found in the list: `{:?}`",
                                possible_flags
                                    .iter()
                                    .map(TypeKey::to_snake_string) // display error in `snake_case`
                                    .collect_vec()
                            )));
                        }
                    }
                    // All elements from `seq` must be consumed at this point.
                    Ok(WastVal::Flags(vec))
                } else {
                    Err(Error::invalid_type(Unexpected::Seq, &self))
                }
            }
        }
        match self.0 {
            TypeWrapper::Option(_) => deserializer.deserialize_option(WastValVisitor(self.0)),
            _ => deserializer.deserialize_any(WastValVisitor(self.0)),
        }
    }
}

pub mod params {
    use crate::{type_wrapper::TypeWrapper, wast_val::WastVal, wast_val_ser::WastValDeserialize};
    use core::fmt;
    use itertools::{Itertools as _, Position};
    use serde::{
        Deserializer,
        de::{Expected, SeqAccess, Visitor},
    };
    use serde_json::Value;

    // Visitor implementation that deserializes a JSON array into `Vec<WastVal>`.
    struct SequenceVisitor<'a, I: ExactSizeIterator<Item = &'a TypeWrapper>> {
        iterator: I,
        len: usize,
        current_idx: usize,
    }

    impl<'a, I: ExactSizeIterator<Item = &'a TypeWrapper>> SequenceVisitor<'a, I> {
        fn new(iterator: I) -> Self {
            let len = iterator.len();
            Self {
                iterator,
                len,
                current_idx: 0,
            }
        }
    }
    impl<'a, 'de, I: ExactSizeIterator<Item = &'a TypeWrapper>> Visitor<'de>
        for SequenceVisitor<'a, I>
    {
        type Value = Vec<WastVal>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(formatter, "an array of length {}", self.len)
        }

        fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            use serde::de::Error;
            let mut vec = Vec::new();
            while let Some(ty) = self.iterator.next() {
                self.current_idx += 1; // parameters are usually indexed from 1.
                if let Some(val) = seq
                    .next_element_seed(WastValDeserialize(ty))
                    .map_err(|err| {
                        Error::custom(format_args!(
                            "cannot parse {idx}-th parameter - `{err}`",
                            idx = self.current_idx
                        ))
                    })?
                {
                    vec.push(val);
                } else {
                    Err(Error::invalid_length(vec.len(), &self))?;
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

    pub fn deserialize_str<'a>(
        params: impl AsRef<str>,
        param_types: impl IntoIterator<
            Item = &'a TypeWrapper,
            IntoIter = impl ExactSizeIterator<Item = &'a TypeWrapper>,
        >,
    ) -> Result<Vec<WastVal>, serde_json::Error> {
        let mut deserializer = serde_json::Deserializer::from_str(params.as_ref());
        let visitor = SequenceVisitor::new(param_types.into_iter());
        deserializer.deserialize_seq(visitor)
    }

    pub fn deserialize_values<'a>(
        params: &[Value],
        param_types: impl IntoIterator<
            Item = &'a TypeWrapper,
            IntoIter = impl ExactSizeIterator<Item = &'a TypeWrapper>,
        >,
    ) -> Result<Vec<WastVal>, serde_json::Error> {
        let params = params
            .iter()
            .map(std::string::ToString::to_string)
            .with_position()
            .fold("[".to_string(), |mut acc, (pos, item)| {
                acc.push_str(&item.clone());
                if pos != Position::Last && pos != Position::Only {
                    acc.push(',');
                } else {
                    acc.push(']');
                }
                acc
            });
        deserialize_str(params, param_types) //FIXME: serializing to string and then deserializing
    }
}

pub fn deserialize_slice(
    slice: &[u8],
    type_wrapper: TypeWrapper,
) -> Result<WastValWithType, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_slice(slice);
    let seed = WastValDeserialize(&type_wrapper);
    seed.deserialize(&mut deserializer)
        .map(|value| WastValWithType {
            r#type: type_wrapper,
            value,
        })
}

pub fn deserialize_value(
    value: &serde_json::Value,
    ty: TypeWrapper,
) -> Result<WastValWithType, serde_json::Error> {
    let value = value.to_string(); //FIXME: serializing to string and then deserializing
    let mut deserializer = serde_json::Deserializer::from_str(value.as_ref());
    let seed = WastValDeserialize(&ty);
    seed.deserialize(&mut deserializer)
        .map(|value| WastValWithType { r#type: ty, value })
}

#[cfg(test)]
mod tests {
    use super::params::deserialize_str;
    use crate::{
        type_wrapper::{TypeKey, TypeWrapper},
        wast_val::{ValKey, WastVal, WastValWithType},
        wast_val_ser::WastValDeserialize,
    };
    use assert_matches::assert_matches;
    use indexmap::{IndexMap, indexmap, indexset};
    use itertools::Itertools;
    use serde::de::DeserializeSeed;

    #[test]
    fn bool() {
        let expected = WastVal::Bool(true);
        let input = "true";
        let ty = TypeWrapper::Bool;
        let actual = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn string() {
        let expected = WastVal::String("test".into());
        let input = r#""test""#;
        let ty = TypeWrapper::String;
        let actual = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn u8() {
        let expected = WastVal::U8(123);
        let input = "123";
        let ty = TypeWrapper::U8;
        let actual = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn s8() {
        let expected = WastVal::S8(-123);
        let input = "-123";
        let ty = TypeWrapper::S8;
        let actual = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn f32() {
        // -123.5 has finite binary fraction (`-0b1111011.1`) so it is exact in
        // both f32 and f64 — round-trip passes.
        let expected = WastVal::F32(-123.5_f32);
        let input = "-123.5";
        let ty = TypeWrapper::F32;
        let actual = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn f32_max() {
        let expected = WastVal::F32(f32::MAX);
        // Use the canonical f64-text of f32::MAX so the f64 parsed by serde
        // equals `f64::from(f32::MAX)` exactly, allowing the round-trip
        // through f32 to confirm losslessness.
        let input = f64::from(f32::MAX).to_string();
        let ty = TypeWrapper::F32;
        let actual = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(&input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn lossy_float_into_f32_fails() {
        // 0.1 has different bit patterns in f64 and f32 (both inexact, but
        // approximating to different precisions), so f64::from(0.1f32) ≠ 0.1f64.
        let err = WastValDeserialize(&TypeWrapper::F32)
            .deserialize(&mut serde_json::Deserializer::from_str("0.1"))
            .unwrap_err();
        assert_starts_with(&err, "invalid type: floating point `0.1`");
    }

    #[test]
    fn whole_float_with_low_bits_into_f32_fails() {
        // 2^24 + 1 = 16777217 is exactly representable in f64 but not f32
        // (f32 mantissa is 24 bits). Same as the int → f32 case above, just
        // via the float branch.
        let err = WastValDeserialize(&TypeWrapper::F32)
            .deserialize(&mut serde_json::Deserializer::from_str("16777217.0"))
            .unwrap_err();
        assert_starts_with(&err, "invalid type: floating point `16777217.0`");
    }

    #[test]
    fn f64() {
        let f = f64::from(f32::MAX) + 1.0;
        let expected = WastVal::F64(f);
        let input = f.to_string();
        let ty = TypeWrapper::F64;
        let actual = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(&input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn f32_from_f64_overflow1() {
        let f = f64::from(f32::MAX) * 2.0;
        let input = f.to_string();
        let ty = TypeWrapper::F32;
        let err = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(&input))
            .unwrap_err();
        assert_starts_with(
            &err,
            "invalid type: floating point `6.805646932770577e+38`, expected value matching \"f32\"",
        );
    }

    #[test]
    fn f64_out_of_range() {
        let input = f64::MAX.to_string();
        let ty = TypeWrapper::F64;
        let err = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(&input))
            .unwrap_err();
        assert_starts_with(&err, "number out of range");
    }

    // The next block of tests cover JS Number ↔ WIT numeric crossing: serde_json
    // parses `42.0` (decimal present) as f64 and `42` (no decimal) as int. JS
    // components reach us through Boa's `JsValue::to_json`, which preserves
    // Boa's internal int32-vs-float tag — so a whole-number JS value above
    // `2^31 - 1` arrives as `42.0` even when intended as an integer, and a
    // small literal returned into an `f64` field arrives as `42`. Both must
    // be accepted as long as the value losslessly fits the target type.

    #[test]
    fn whole_float_into_s32() {
        let actual = WastValDeserialize(&TypeWrapper::S32)
            .deserialize(&mut serde_json::Deserializer::from_str("42.0"))
            .unwrap();
        assert_eq!(WastVal::S32(42), actual);
    }

    #[test]
    fn whole_float_into_u32() {
        // 2^31 — out of int32 range, valid u32.
        let actual = WastValDeserialize(&TypeWrapper::U32)
            .deserialize(&mut serde_json::Deserializer::from_str("2147483648.0"))
            .unwrap();
        assert_eq!(WastVal::U32(2_147_483_648), actual);
    }

    #[test]
    fn whole_float_into_s64() {
        let actual = WastValDeserialize(&TypeWrapper::S64)
            .deserialize(&mut serde_json::Deserializer::from_str("9999999999.0"))
            .unwrap();
        assert_eq!(WastVal::S64(9_999_999_999), actual);
    }

    #[test]
    fn whole_float_negative_into_s32() {
        let actual = WastValDeserialize(&TypeWrapper::S32)
            .deserialize(&mut serde_json::Deserializer::from_str("-42.0"))
            .unwrap();
        assert_eq!(WastVal::S32(-42), actual);
    }

    #[test]
    fn fractional_float_into_s32_still_fails() {
        let err = WastValDeserialize(&TypeWrapper::S32)
            .deserialize(&mut serde_json::Deserializer::from_str("42.5"))
            .unwrap_err();
        assert_starts_with(
            &err,
            "invalid type: floating point `42.5`, expected value matching \"s32\"",
        );
    }

    #[test]
    fn negative_float_into_u32_still_fails() {
        let err = WastValDeserialize(&TypeWrapper::U32)
            .deserialize(&mut serde_json::Deserializer::from_str("-1.0"))
            .unwrap_err();
        // Routes through visit_i64 (whole-number trunc), which rejects -1 as out-of-range for u32.
        assert_starts_with(&err, "invalid type: integer `-1`");
    }

    #[test]
    fn oversized_float_into_s8_still_fails() {
        let err = WastValDeserialize(&TypeWrapper::S8)
            .deserialize(&mut serde_json::Deserializer::from_str("1000.0"))
            .unwrap_err();
        // Routes through visit_u64 which rejects it as out-of-range for s8.
        assert_starts_with(&err, "invalid type: integer `1000`");
    }

    #[test]
    fn int_into_f64() {
        let actual = WastValDeserialize(&TypeWrapper::F64)
            .deserialize(&mut serde_json::Deserializer::from_str("42"))
            .unwrap();
        assert_eq!(WastVal::F64(42.0), actual);
    }

    #[test]
    fn negative_int_into_f64() {
        let actual = WastValDeserialize(&TypeWrapper::F64)
            .deserialize(&mut serde_json::Deserializer::from_str("-42"))
            .unwrap();
        assert_eq!(WastVal::F64(-42.0), actual);
    }

    #[test]
    fn int_into_f32() {
        let actual = WastValDeserialize(&TypeWrapper::F32)
            .deserialize(&mut serde_json::Deserializer::from_str("42"))
            .unwrap();
        assert_eq!(WastVal::F32(42.0), actual);
    }

    #[test]
    fn large_int_into_f64() {
        // Number.MAX_SAFE_INTEGER from JS.
        let actual = WastValDeserialize(&TypeWrapper::F64)
            .deserialize(&mut serde_json::Deserializer::from_str("9007199254740991"))
            .unwrap();
        assert_eq!(WastVal::F64(9_007_199_254_740_991.0), actual);
    }

    #[test]
    fn lossy_int_into_f64_fails() {
        // 2^53 + 1 cannot be represented in f64. Reject rather than silently round.
        let err = WastValDeserialize(&TypeWrapper::F64)
            .deserialize(&mut serde_json::Deserializer::from_str("9007199254740993"))
            .unwrap_err();
        assert_starts_with(&err, "invalid type: integer `9007199254740993`");
    }

    #[test]
    fn lossy_int_into_f32_fails() {
        // 2^24 + 1 cannot be represented in f32.
        let err = WastValDeserialize(&TypeWrapper::F32)
            .deserialize(&mut serde_json::Deserializer::from_str("16777217"))
            .unwrap_err();
        assert_starts_with(&err, "invalid type: integer `16777217`");
    }

    #[test]
    fn lossless_pow2_int_into_f32() {
        // 2^24 itself is exact in f32.
        let actual = WastValDeserialize(&TypeWrapper::F32)
            .deserialize(&mut serde_json::Deserializer::from_str("16777216"))
            .unwrap();
        assert_eq!(WastVal::F32(16_777_216.0), actual);
    }

    #[test]
    fn u64_max_into_f64_fails() {
        // u64::MAX = 2^64 - 1. `as f64` rounds up to 2^64, and the saturating
        // cast back to u64 returns u64::MAX. The u128 round-trip catches this
        // false-positive.
        let err = WastValDeserialize(&TypeWrapper::F64)
            .deserialize(&mut serde_json::Deserializer::from_str(
                "18446744073709551615",
            ))
            .unwrap_err();
        assert_starts_with(&err, "invalid type: integer `18446744073709551615`");
    }

    #[test]
    fn i64_min_into_f64() {
        // i64::MIN = -2^63, exactly representable in f64.
        let actual = WastValDeserialize(&TypeWrapper::F64)
            .deserialize(&mut serde_json::Deserializer::from_str(
                "-9223372036854775808",
            ))
            .unwrap();
        assert_eq!(WastVal::F64(-9_223_372_036_854_775_808.0), actual);
    }

    #[test]
    fn i64_max_into_f64_fails() {
        // i64::MAX = 2^63 - 1. `as f64` rounds up to 2^63, then saturating
        // cast back to i64 returns i64::MAX. i128 round-trip catches it.
        let err = WastValDeserialize(&TypeWrapper::F64)
            .deserialize(&mut serde_json::Deserializer::from_str(
                "9223372036854775807",
            ))
            .unwrap_err();
        assert_starts_with(&err, "invalid type: integer `9223372036854775807`");
    }

    // The reverse-direction saturation case: input is `2^64` as a JSON float,
    // serde reads it as f64, visit_f64 routes through f64_to_u64_exact. A
    // naive `val as u64` would saturate to u64::MAX (= 2^64 - 1) and lie via
    // the float round-trip (u64::MAX as f64 rounds back up to 2^64). Routing
    // through u128 + try_from rejects cleanly.
    #[test]
    fn pow2_64_float_into_u64_fails() {
        let err = WastValDeserialize(&TypeWrapper::U64)
            .deserialize(&mut serde_json::Deserializer::from_str(
                "18446744073709551616.0",
            ))
            .unwrap_err();
        assert_starts_with(
            &err,
            "invalid type: floating point `1.8446744073709552e+19`",
        );
    }

    #[test]
    fn pow2_63_float_into_s64_fails() {
        // i64::MAX + 1 as a float — must be refused for s64. Routes through
        // f64_to_u64_exact (2^63 fits in u64) → visit_u64 → try_from s64 fails.
        let err = WastValDeserialize(&TypeWrapper::S64)
            .deserialize(&mut serde_json::Deserializer::from_str(
                "9223372036854775808.0",
            ))
            .unwrap_err();
        assert_starts_with(&err, "invalid type: floating point `9.223372036854776e+18`");
    }

    #[test]
    fn neg_pow2_63_float_into_s64_fails() {
        // i64::MIN = -2^63 is exactly representable as f64, but JSON parsing
        // has already rounded the decimal by `visit_f64`, so values outside
        // the safe integer range must be rejected rather than guessed at.
        let err = WastValDeserialize(&TypeWrapper::S64)
            .deserialize(&mut serde_json::Deserializer::from_str(
                "-9223372036854775808.0",
            ))
            .unwrap_err();
        assert_starts_with(
            &err,
            "invalid type: floating point `-9.223372036854776e+18`",
        );
    }

    #[test]
    fn rounded_float_into_u64_fails() {
        // serde_json rounds this to 9007199254740992.0 before visit_f64. Do
        // not accept the rounded value as if the original decimal were exact.
        let err = WastValDeserialize(&TypeWrapper::U64)
            .deserialize(&mut serde_json::Deserializer::from_str(
                "9007199254740993.0",
            ))
            .unwrap_err();
        assert_starts_with(&err, "invalid type: floating point `900719925474099");
    }

    #[test]
    fn rounded_float_into_s64_fails() {
        let err = WastValDeserialize(&TypeWrapper::S64)
            .deserialize(&mut serde_json::Deserializer::from_str(
                "-9007199254740993.0",
            ))
            .unwrap_err();
        assert_starts_with(&err, "invalid type: floating point `-900719925474099");
    }

    #[test]
    fn very_large_float_into_s64_fails() {
        // f64 > i64::MAX by many orders of magnitude. f as i128 saturates,
        // try_from rejects.
        let err = WastValDeserialize(&TypeWrapper::S64)
            .deserialize(&mut serde_json::Deserializer::from_str("1e30"))
            .unwrap_err();
        assert_starts_with(&err, "invalid type: floating point `1e+30`,");
    }

    #[test]
    fn test_deserialize_sequence() {
        let expected = vec![WastVal::Bool(true), WastVal::U8(8), WastVal::S16(-16)];
        let param_types = r#"["bool", "u8", "s16"]"#;
        let param_vals = "[true, 8, -16]";
        let param_types: Vec<TypeWrapper> = serde_json::from_str(param_types).unwrap();
        let actual = deserialize_str(param_vals, param_types.iter()).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn deserialize_sequence_too_many_types() {
        let param_types = r#"["bool", "u8", "s16"]"#;
        let param_vals = "[true, 8]";
        let param_types: Vec<TypeWrapper> = serde_json::from_str(param_types).unwrap();
        let err = deserialize_str(param_vals, &param_types).unwrap_err();
        assert_starts_with(&err, "invalid length 2, expected an array of length 3");
    }

    #[test]
    fn deserialize_sequence_too_many_values() {
        let param_types = r#"["bool", "u8"]"#;
        let param_vals = "[true, 8, false]";
        let param_types: Vec<TypeWrapper> = serde_json::from_str(param_types).unwrap();
        let err = deserialize_str(param_vals, &param_types).unwrap_err();
        assert_starts_with(
            &err,
            "invalid length that is too big, at element `Bool(false)`, expected an array of length 2",
        );
    }

    fn assert_starts_with(err: &dyn std::error::Error, starts_with: &str) {
        let actual = err.to_string();
        assert!(
            actual.starts_with(starts_with),
            "assert_starts_with failed.Got:\n{actual}\nExpected to start with:\n{starts_with}"
        );
    }

    #[test]
    fn serde_bool() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Bool,
            value: WastVal::Bool(true),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_result_ok_none() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Result {
                ok: None,
                err: None,
            },
            value: WastVal::Result(Ok(None)),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_result_ok_option_none() {
        // Note: this does not trigger the bug, only `join_set_deser_with_result_ok_option_none_should_work`
        let expected = WastValWithType {
            r#type: TypeWrapper::Result {
                ok: Some(Box::new(TypeWrapper::Option(Box::new(TypeWrapper::String)))),
                err: Some(Box::new(TypeWrapper::String)),
            },
            value: WastVal::Result(Ok(Some(Box::new(WastVal::Option(None))))),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_result_ok_none_invalid_bool() {
        let json = r#"
            {"type":"result","value":{"ok":true}}
            "#;
        let actual = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "invalid result value, expected null at line 2 column 48",
            actual.to_string()
        );
    }

    #[test]
    fn serde_result_ok_none_invalid_string_value() {
        let json = r#"
            {"type":"result","value":{"ok": "str"}}
            "#;
        let actual = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "invalid result value, expected null at line 2 column 50",
            actual.to_string()
        );
    }

    #[test]
    fn serde_result_err_none_invalid_bool() {
        let json = r#"
            {"type":"result","value":{"err":true}}
            "#;
        let actual = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "invalid result value, expected null at line 2 column 49",
            actual.to_string()
        );
    }

    #[test]
    fn serde_result_err_none_invalid_string() {
        let json = r#"
            {"type":"result","value":{"err": "str"}}
            "#;
        let actual = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "invalid result value, expected null at line 2 column 51",
            actual.to_string()
        );
    }

    #[test]
    fn serde_result_ok_some() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Result {
                ok: Some(TypeWrapper::Bool.into()),
                err: None,
            },
            value: WastVal::Result(Ok(Some(WastVal::Bool(true).into()))),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_result_err_none() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Result {
                ok: None,
                err: None,
            },
            value: WastVal::Result(Err(None)),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_result_err_some() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Result {
                ok: Some(TypeWrapper::Bool.into()),
                err: Some(TypeWrapper::String.into()),
            },
            value: WastVal::Result(Err(Some(WastVal::String("test".into()).into()))),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_option_some() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Option(TypeWrapper::Bool.into()),
            value: WastVal::Option(Some(WastVal::Bool(true).into())),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_option_none() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Option(TypeWrapper::Bool.into()),
            value: WastVal::Option(None),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_list() {
        let expected = WastValWithType {
            r#type: TypeWrapper::List(TypeWrapper::Bool.into()),
            value: WastVal::List(vec![WastVal::Bool(true)]),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_record() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Record(IndexMap::from([
                (TypeKey::new_kebab("field1"), TypeWrapper::Bool),
                (TypeKey::new_kebab("field-2"), TypeWrapper::U32),
            ])),
            value: WastVal::Record(IndexMap::from([
                (ValKey::new_snake("field1"), WastVal::Bool(true)),
                (ValKey::new_snake("field_2".to_string()), WastVal::U32(1)),
            ])),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_variant_with_field() {
        let duration_type_wrapper = TypeWrapper::Variant(indexmap! {
            TypeKey::new_kebab("milli-seconds") => Some(TypeWrapper::U64),
            TypeKey::new_kebab("seconds") => Some(TypeWrapper::U64),
            TypeKey::new_kebab("minutes") => Some(TypeWrapper::U32),
            TypeKey::new_kebab("hours") => Some(TypeWrapper::U32),
            TypeKey::new_kebab("days") => Some(TypeWrapper::U32),
        });
        let expected = WastValWithType {
            r#type: duration_type_wrapper,
            value: WastVal::Variant(
                ValKey::new_snake("milli_seconds"),
                Some(Box::new(WastVal::U64(1))),
            ),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_variant_without_field() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Variant(indexmap! {
                TypeKey::new_kebab("a") => None,
                TypeKey::new_kebab("b") => None,
            }),
            value: WastVal::Variant(ValKey::new_snake("a"), None),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_variant_wrong_tag() {
        let json = r#"
            {"type":"variant { a, b }","value":{"c":null}}
            "#;
        let err = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "cannot deserialize variant: `c` not found in the following list: `[\"a\", \"b\"]` at line 2 column 51",
            err.to_string()
        );
    }

    #[test]
    fn serde_variant_unexpected_string() {
        let json = r#"
            {"type":"variant { string, map(u64) }","value":"map"}
        "#;
        let err = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "cannot deserialize variant: `map` must be serialized as map at line 2 column 64",
            err.to_string()
        );
    }

    #[test]
    fn serde_variant_unexpected_map() {
        let json = r#"
            {"type":"variant { string, map(u64) }","value":{"string": true}}
        "#;
        let err = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "cannot deserialize variant: `string` must be serialized as string at line 2 column 68",
            err.to_string()
        );
    }

    #[test]
    fn serde_enum() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Enum(indexset! {
                TypeKey::new_kebab("a-a"),
                TypeKey::new_kebab("b"),
            }),
            value: WastVal::Enum(ValKey::new_snake("a_a")),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_enum_deser_wrong_key_should_fail() {
        let json = r#"
            {"type":"enum { a, b }","value":"c"}
            "#;
        let err = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "cannot deserialize enum: `c` not found in the following list: `[\"a\", \"b\"]` at line 2 column 47",
            err.to_string()
        );
    }

    #[test]
    fn serde_enum_deser_map_should_expect_string() {
        let json = r#"
            {"type":"enum { a, b }","value":{"a":1}}
            "#;
        let err = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "invalid type: map, expected value matching \"enum { a, b }\" at line 2 column 45",
            err.to_string()
        );
    }

    #[test]
    fn serde_enum_deser_with_too_few_values_should_fail() {
        let json = r#"{"type":"tuple<bool, u32>","value":[false]}"#;
        let err = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "invalid length 1, expected value matching \"tuple<bool, u32>\" at line 1 column 42",
            err.to_string()
        );
    }

    #[test]
    fn serde_enum_deser_with_too_many_values_should_fail() {
        let json = r#"{"type":"tuple<bool, u32>","value":[false, 1, false]}"#;
        let err = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "invalid length 3, expected value matching \"tuple<bool, u32>\" at line 1 column 52",
            err.to_string()
        );
    }

    #[test]
    fn serde_tuple() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Tuple(Box::new([TypeWrapper::Bool, TypeWrapper::U32])),
            value: WastVal::Tuple(vec![WastVal::Bool(false), WastVal::U32(1)]),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_flags() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Flags(
                indexset! {TypeKey::new_kebab("a-a"), TypeKey::new_kebab("b"), TypeKey::new_kebab("c")},
            ),
            value: WastVal::Flags(vec![ValKey::new_snake("a_a"), ValKey::new_snake("b")]),
        };
        let json = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(json);
        let actual = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_flags_duplicates_should_fail() {
        let json = r#"{"type":"flags { a, b, c }","value":["a","a"]}"#;
        let err = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "cannot deserialize flags: flag `a` was found more than once at line 1 column 45",
            err.to_string()
        );
    }

    #[test]
    fn serde_flags_unknown_flags_should_fail() {
        let json = r#"{"type":"flags { a, b, c }","value":["a","d"]}"#;
        let err = serde_json::from_str::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "cannot deserialize flags: flag `d` not found in the list: `[\"a\", \"b\", \"c\"]` at line 1 column 45",
            err.to_string()
        );
    }

    #[test]
    fn deser_should_preserve_its_attribute_order() {
        let expected_keys = vec!["logins", "cursor"];
        let json = r#"
            {
                "type": "record { logins: string, cursor: string }",
                "value": {
                    "logins": "logins",
                    "cursor": "cursor"
                }
            }
        "#;
        let deser: WastValWithType = serde_json::from_str(json).unwrap();
        let fields = assert_matches!(deser.value, WastVal::Record(fields) => fields);
        assert_eq!(
            expected_keys,
            fields
                .keys()
                .map(super::super::wast_val::ValKey::to_kebab_string)
                .collect_vec()
        );

        let fields = assert_matches!(deser.r#type, TypeWrapper::Record(fields) => fields);
        assert_eq!(
            expected_keys,
            fields.keys().map(TypeKey::as_kebab_str).collect_vec()
        );
    }
}
