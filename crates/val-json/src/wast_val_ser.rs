use crate::{
    type_wrapper::TypeWrapper,
    wast_val::{WastVal, WastValWithType},
};
use indexmap::IndexMap;
use serde::{
    de::{self, DeserializeSeed, Deserializer, MapAccess, Visitor},
    Deserialize, Serialize, Serializer,
};

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
            WastVal::Variant(key, None) | WastVal::Enum(key) => serializer.serialize_str(key),
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
                    if let Some(t) = &r#type {
                        value = Some(map.next_value_seed(WastValDeserialize(t))?);
                    } else {
                        return Err(de::Error::custom("`type` field must preceed `value` field"));
                    }
                }
            }
        }
        let r#type = r#type.ok_or_else(|| de::Error::missing_field("type"))?;
        let value = value.ok_or_else(|| de::Error::missing_field("value"))?;
        Ok(Self::Value { r#type, value })
    }
}

struct WastValDeserialize<'a>(&'a TypeWrapper);

impl<'de> DeserializeSeed<'de> for WastValDeserialize<'_> {
    type Value = WastVal;

    #[expect(clippy::too_many_lines)]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Unexpected};

        struct WastValVisitor<'a>(&'a TypeWrapper);

        impl<'de> Visitor<'de> for WastValVisitor<'_> {
            type Value = WastVal;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "value matching {type:?}", type = self.0) // TODO: wit_type instead of TypeWrapper
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
                    Ok(WastVal::F64(val))
                } else if *self.0 == TypeWrapper::F32 {
                    // Warining: This might truncate the value.
                    #[expect(clippy::cast_possible_truncation)]
                    let f32 = val as f32;
                    // Fail on overflow.
                    if val.is_finite() == f32.is_finite() {
                        return Ok(WastVal::F32(f32));
                    }
                    Err(())
                } else {
                    Err(())
                }
                .map_err(|()| Error::invalid_type(Unexpected::Float(val), &self))
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
                    TypeWrapper::Variant(variants) => {
                        if let Some(found) = variants.get(str_val) {
                            if found.is_none() {
                                Ok(WastVal::Variant(str_val.to_string(), None))
                            } else {
                                Err(Error::custom(format!(
                                    "cannot deserialize variant: `{str_val}` must be serialized as map"
                                )))
                            }
                        } else {
                            Err(Error::custom(format!("cannot deserialize enum: `{str_val}` not found in the following list: `{variants:?}`")))
                        }
                    }
                    TypeWrapper::Enum(variants) => {
                        if variants.contains(str_val) {
                            Ok(WastVal::Enum(str_val.to_string()))
                        } else {
                            Err(Error::custom(format!("cannot deserialize enum: `{str_val}` not found in the following list: `{variants:?}`")))
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
                                let dummy_seed = TypeWrapper::Option(TypeWrapper::Bool.into());
                                let value_seed = WastValDeserialize(&dummy_seed);
                                let value = map.next_value_seed(value_seed)?;
                                if matches!(value, WastVal::Option(None)) {
                                    Ok(WastVal::Result(Ok(None)))
                                } else {
                                    Err(Error::custom("invalid result value, expected null"))
                                }
                            }
                        } else if ok_or_err == "err" {
                            if let Some(err) = err {
                                let err = map.next_value_seed(WastValDeserialize(err))?;
                                Ok(WastVal::Result(Err(Some(err.into()))))
                            } else {
                                let dummy_seed = TypeWrapper::Option(TypeWrapper::Bool.into());
                                let value_seed = WastValDeserialize(&dummy_seed);
                                let value = map.next_value_seed(value_seed)?;
                                if matches!(value, WastVal::Option(None)) {
                                    Ok(WastVal::Result(Err(None)))
                                } else {
                                    Err(Error::custom("invalid result value, expected null"))
                                }
                            }
                        } else {
                            Err(Error::custom("expected one of `ok`, `err`"))
                        }
                    }
                    TypeWrapper::Record(record) => {
                        let mut dst_map = IndexMap::new();
                        while let Some(field_name) = map.next_key::<String>()? {
                            if let Some(field_type) = record.get(field_name.as_str()) {
                                let value = map.next_value_seed(WastValDeserialize(field_type))?;
                                dst_map.insert(field_name, value);
                            } else {
                                return Err(Error::custom(format!(
                                    "unexpected field name `{field_name}`"
                                )));
                            }
                        }
                        Ok(WastVal::Record(dst_map))
                    }
                    TypeWrapper::Variant(variants) => {
                        if let Some(variant_name) = map.next_key::<String>()? {
                            if let Some(found) = variants.get(variant_name.as_str()) {
                                if let Some(variant_field_type) = found {
                                    let value = map
                                        .next_value_seed(WastValDeserialize(variant_field_type))?;
                                    Ok(WastVal::Variant(variant_name, Some(Box::new(value))))
                                } else {
                                    Err(Error::custom(format!(
                                        "cannot deserialize variant: `{variant_name}` must be serialized as string"
                                    )))
                                }
                            } else {
                                Err(Error::custom(format!(
                                    "cannot deserialize variant: `{variant_name}` not found in the following list: `{:?}`",
                                    variants.keys().collect::<Vec<_>>()
                                )))
                            }
                        } else {
                            Err(Error::custom(format!(
                                "cannot deserialize variant: expected single map key from following list: `{:?}`",
                                variants.keys().collect::<Vec<_>>()
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
                    if let Some(actual_size) = seq.size_hint() {
                        if actual_size != type_sequence.len() {
                            return Err(Error::invalid_length(actual_size, &self));
                        }
                    }
                    let mut vec = Vec::new();
                    for (idx, seed) in type_sequence.iter().enumerate() {
                        if let Some(element) = seq.next_element_seed(WastValDeserialize(seed))? {
                            vec.push(element);
                        } else {
                            return Err(Error::invalid_length(idx, &self));
                        }
                    }
                    // Will fail if there are more elements than consumed
                    Ok(WastVal::Tuple(vec))
                } else if let TypeWrapper::Flags(possible_flags) = self.0 {
                    let mut vec = Vec::with_capacity(possible_flags.len());
                    while let Some(element) = seq.next_element::<String>()? {
                        if vec.contains(&element) {
                            return Err(Error::custom(format!("cannot deserialize flags: flag `{element}` was found more than once")));
                        } else if possible_flags.contains(element.as_str()) {
                            vec.push(element);
                        } else {
                            return Err(Error::custom(format!("cannot deserialize flags: flag `{element}` not found in the list: `{possible_flags:?}`")));
                        }
                    }
                    // Will fail if there are more elements than consumed
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
        de::{Expected, SeqAccess, Visitor},
        Deserializer,
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
                acc.push_str(&item.to_string());
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

#[cfg(test)]
mod tests {
    use super::params::deserialize_str;
    use crate::{
        type_wrapper::TypeWrapper,
        wast_val::{WastVal, WastValWithType},
        wast_val_ser::WastValDeserialize,
    };
    use indexmap::{indexmap, indexset, IndexMap};
    use serde::de::DeserializeSeed;
    use serde_json::json;

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
        let expected = WastVal::F32(-123.1_f32);
        let input = "-123.1";
        let ty = TypeWrapper::F32;
        let actual = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(input))
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn f32_max() {
        let expected = WastVal::F32(f32::MAX);
        let input = f32::MAX.to_string();
        let ty = TypeWrapper::F32;
        let actual = WastValDeserialize(&ty)
            .deserialize(&mut serde_json::Deserializer::from_str(&input))
            .unwrap();
        assert_eq!(expected, actual);
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
            "invalid type: floating point `6.805646932770577e38`, expected value matching F32",
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
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(json!({"type":"bool","value":true}), json);
        let actual = serde_json::from_value(json).unwrap();
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
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(
            json!({"type":{"result":{"ok":null, "err":null}},"value":{"ok":null}}),
            json
        );
        let actual = serde_json::from_value(json).unwrap();
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
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(
            json!({"type":{"result":{"ok":{"option":"string"},"err":"string"}},"value":{"ok":null}}),
            json
        );
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_result_none_invalid() {
        let json =
            serde_json::json!({"type":{"result":{"err":null,"ok":null}},"value":{"ok":true}});
        let actual = serde_json::from_value::<WastValWithType>(json).unwrap_err();
        assert_eq!("invalid result value, expected null", actual.to_string());
        let json =
            serde_json::json!({"type":{"result":{"err":null,"ok":null}},"value":{"err":true}});
        let actual = serde_json::from_value::<WastValWithType>(json).unwrap_err();
        assert_eq!("invalid result value, expected null", actual.to_string());
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
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(
            json!({"type":{"result":{"ok":"bool", "err":null}},"value":{"ok":true}}),
            json
        );
        let actual = serde_json::from_value(json).unwrap();
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
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(
            json!({"type":{"result":{"ok":null, "err":null}},"value":{"err":null}}),
            json
        );
        let actual = serde_json::from_value(json).unwrap();
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
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(
            json!({"type":{"result":{"ok":"bool", "err":"string"}},"value":{"err":"test"}}),
            json
        );
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_option_some() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Option(TypeWrapper::Bool.into()),
            value: WastVal::Option(Some(WastVal::Bool(true).into())),
        };
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(json!({"type":{"option":"bool"},"value":true}), json);
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_option_none() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Option(TypeWrapper::Bool.into()),
            value: WastVal::Option(None),
        };
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(json!({"type":{"option":"bool"},"value":null}), json);
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_list() {
        let expected = WastValWithType {
            r#type: TypeWrapper::List(TypeWrapper::Bool.into()),
            value: WastVal::List(vec![WastVal::Bool(true)]),
        };
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(json!({"type":{"list":"bool"},"value":[true]}), json);
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_record() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Record(IndexMap::from([
                (Box::from("field1"), TypeWrapper::Bool),
                (Box::from("field2"), TypeWrapper::U32),
            ])),
            value: WastVal::Record(IndexMap::from([
                ("field1".to_string(), WastVal::Bool(true)),
                ("field2".to_string(), WastVal::U32(1)),
            ])),
        };
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(
            json!({"type":{"record":{"field1":"bool","field2":"u32"}},"value":{"field1":true,"field2":1}}),
            json
        );
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_variant_with_field() {
        let duration_type_wrapper = TypeWrapper::Variant(indexmap! {
            Box::from("milliseconds") => Some(TypeWrapper::U64),
            Box::from("seconds") => Some(TypeWrapper::U64),
            Box::from("minutes") => Some(TypeWrapper::U32),
            Box::from("hours") => Some(TypeWrapper::U32),
            Box::from("days") => Some(TypeWrapper::U32),
        });
        let expected = WastValWithType {
            r#type: duration_type_wrapper,
            value: WastVal::Variant("seconds".to_string(), Some(Box::new(WastVal::U64(1)))),
        };
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(
            json!({"type":{"variant":{"milliseconds":"u64","seconds":"u64","minutes":"u32","hours":"u32","days":"u32"}},
                "value":{"seconds":1}}),
            json
        );
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_variant_without_field() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Variant(indexmap! {
                Box::from("a") => None,
                Box::from("b") => None,
            }),
            value: WastVal::Variant("a".to_string(), None),
        };
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(
            json!({"type":{"variant":{"a":null,"b":null}},"value":"a"}),
            json
        );
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_variant_wrong_tag() {
        let err = serde_json::from_value::<WastValWithType>(
            json!({"type":{"variant":{"a":null,"b":null}},"value":{"c":null}}),
        )
        .unwrap_err();
        assert_eq!(
            "cannot deserialize variant: `c` not found in the following list: `[\"a\", \"b\"]`",
            err.to_string()
        );
    }

    #[test]
    fn serde_variant_unexpected_string() {
        let err = serde_json::from_value::<WastValWithType>(
            json!({"type":{"variant":{"string":null,"map":"u64"}},"value":"map"}),
        )
        .unwrap_err();
        assert_eq!(
            "cannot deserialize variant: `map` must be serialized as map",
            err.to_string()
        );
    }

    #[test]
    fn serde_variant_unexpected_map() {
        let err = serde_json::from_value::<WastValWithType>(
            json!({"type":{"variant":{"string":null,"map":"u64"}},"value":{"string": true}}),
        )
        .unwrap_err();
        assert_eq!(
            "cannot deserialize variant: `string` must be serialized as string",
            err.to_string()
        );
    }

    #[test]
    fn serde_enum() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Enum(indexset! {
                Box::from("a"),
                Box::from("b"),
            }),
            value: WastVal::Enum("a".to_string()),
        };
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(json!({"type":{"enum":["a","b"]},"value":"a"}), json);
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_enum_deser_wrong_key_should_fail() {
        let json = json!({"type":{"enum":["a","b"]},"value":"c"});
        let err = serde_json::from_value::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "cannot deserialize enum: `c` not found in the following list: `{\"a\", \"b\"}`",
            err.to_string()
        );
    }

    #[test]
    fn serde_enum_deser_map_should_expect_string() {
        let json = json!({"type":{"enum":["a","b"]},"value":{"a":1}});
        let err = serde_json::from_value::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "invalid type: map, expected value matching Enum({\"a\", \"b\"})",
            err.to_string()
        );
    }

    #[test]
    fn serde_enum_deser_with_too_few_values_should_fail() {
        let json = json!({"type":{"tuple":["bool","u32"]},"value":[false]});
        let err = serde_json::from_value::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "invalid length 1, expected value matching Tuple([Bool, U32])",
            err.to_string()
        );
    }

    #[test]
    fn serde_enum_deser_with_too_many_values_should_fail() {
        let json = json!({"type":{"tuple":["bool","u32"]},"value":[false, 1, false]});
        let err = serde_json::from_value::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "invalid length 3, expected value matching Tuple([Bool, U32])",
            err.to_string()
        );
    }

    #[test]
    fn serde_tuple() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Tuple(Box::new([TypeWrapper::Bool, TypeWrapper::U32])),
            value: WastVal::Tuple(vec![WastVal::Bool(false), WastVal::U32(1)]),
        };
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(
            json!({"type":{"tuple":["bool","u32"]},"value":[false,1]}),
            json,
        );
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_flags() {
        let expected = WastValWithType {
            r#type: TypeWrapper::Flags(indexset! {Box::from("a"), Box::from("b"), Box::from("c")}),
            value: WastVal::Flags(vec!["a".to_string(), "b".to_string()]),
        };
        let json = serde_json::to_value(&expected).unwrap();
        assert_eq!(
            json!({"type":{"flags":["a","b","c"]},"value":["a","b"]}),
            json
        );
        let actual = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serde_flags_duplicates_should_fail() {
        let json = json!({"type":{"flags":["a","b","c"]},"value":["a","a"]});
        let err = serde_json::from_value::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "cannot deserialize flags: flag `a` was found more than once",
            err.to_string()
        );
    }

    #[test]
    fn serde_flags_unknown_flags_should_fail() {
        let json = json!({"type":{"flags":["a","b","c"]},"value":["a","d"]});
        let err = serde_json::from_value::<WastValWithType>(json).unwrap_err();
        assert_eq!(
            "cannot deserialize flags: flag `d` not found in the list: `{\"a\", \"b\", \"c\"}`",
            err.to_string()
        );
    }
}
