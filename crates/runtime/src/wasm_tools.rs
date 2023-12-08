use crate::{FunctionFqn, FunctionMetadata, FunctionMetadataError};
use anyhow::{anyhow, bail};
use std::{borrow::Cow, collections::HashMap};
use val_json::TypeWrapper;

use wit_component::DecodedWasm;

pub(crate) fn exported_interfaces(
    decoded: &'_ DecodedWasm,
) -> Result<
    impl Iterator<
            Item = (
                &'_ wit_parser::PackageName,
                &'_ str,
                &'_ indexmap::IndexMap<String, wit_parser::Function>,
            ),
        > + Clone,
    anyhow::Error,
> {
    let (resolve, world_id) = match decoded {
        DecodedWasm::Component(resolve, world_id) => (resolve, world_id),
        _ => bail!("cannot parse component"),
    };
    let world = resolve
        .worlds
        .get(*world_id)
        .ok_or_else(|| anyhow!("world must exist"))?;
    Ok(world.exports.iter().filter_map(|(_, item)| match item {
        wit_parser::WorldItem::Interface(ifc) => {
            let ifc = resolve
                .interfaces
                .get(*ifc)
                .unwrap_or_else(|| panic!("interface must exist"));
            let package_name = ifc
                .package
                .and_then(|pkg| resolve.packages.get(pkg))
                .map(|p| &p.name)
                .unwrap_or_else(|| panic!("empty packages are not supported"));
            let ifc_name = ifc
                .name
                .as_deref()
                .unwrap_or_else(|| panic!("empty interfaces are not supported"));
            Some((package_name, ifc_name, &ifc.functions))
        }
        _ => None,
    }))
}

pub(crate) fn functions_to_metadata<'a>(
    exported_interfaces: impl Iterator<
        Item = (
            &'a wit_parser::PackageName,
            &'a str,
            &'a indexmap::IndexMap<String, wit_parser::Function>,
        ),
    >,
) -> Result<HashMap<FunctionFqn<'static>, FunctionMetadata>, FunctionMetadataError> {
    let mut functions_to_results = HashMap::new();
    for (package_name, ifc_name, functions) in exported_interfaces.into_iter() {
        let ifc_fqn = format!("{package_name}/{ifc_name}");
        for (function_name, function) in functions.into_iter() {
            let fqn = FunctionFqn {
                ifc_fqn: Cow::Owned(ifc_fqn.clone()),
                function_name: Cow::Owned(function_name.clone()),
            };
            let params = function
                .params
                .iter()
                .map(|(name, ty)| (name.clone(), TypeWrapper::from(*ty)))
                .collect();
            match &function.results {
                wit_parser::Results::Anon(_) => Ok(()),
                wit_parser::Results::Named(named) if named.is_empty() => Ok(()),
                other => Err(FunctionMetadataError::UnsupportedReturnType {
                    fqn: fqn.to_string(),
                    ty: format!("{other:?}"),
                }),
            }?;
            functions_to_results.insert(
                fqn,
                FunctionMetadata {
                    results_len: function.results.len(),
                    params,
                },
            );
        }
    }
    Ok(functions_to_results)
}
