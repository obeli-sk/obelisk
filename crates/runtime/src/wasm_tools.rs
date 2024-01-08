use crate::{FunctionFqn, FunctionMetadata, FunctionMetadataError};
use anyhow::{anyhow, bail};
use std::{borrow::Cow, collections::HashMap, sync::Arc};
use val_json::{TypeWrapper, UnsupportedTypeError};

use wit_component::DecodedWasm;

pub(crate) fn exported_interfaces(
    decoded: &'_ DecodedWasm,
) -> Result<
    impl Iterator<
            Item = (
                &'_ wit_parser::PackageName,
                &'_ str, // ifc_name
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
            &'a str, // ifc_name
            &'a indexmap::IndexMap<String, wit_parser::Function>,
        ),
    >,
) -> Result<HashMap<Arc<FunctionFqn<'static>>, FunctionMetadata>, FunctionMetadataError> {
    let mut functions_to_results = HashMap::new();
    for (package_name, ifc_name, functions) in exported_interfaces.into_iter() {
        let ifc_fqn = if let Some(version) = &package_name.version {
            format!(
                "{namespace}:{name}/{ifc_name}@{version}",
                namespace = package_name.namespace,
                name = package_name.name
            )
        } else {
            format!("{package_name}/{ifc_name}")
        };
        for (function_name, function) in functions.into_iter() {
            let fqn = Arc::new(FunctionFqn {
                ifc_fqn: Cow::Owned(ifc_fqn.clone()),
                function_name: Cow::Owned(function_name.clone()),
            });
            let params = function
                .params
                .iter()
                .map(|(name, ty)| TypeWrapper::try_from(*ty).map(|ty| (name.clone(), ty)))
                .collect::<Result<_, UnsupportedTypeError>>()?;
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

pub(crate) fn is_limit_reached(reason: &str) -> bool {
    reason.starts_with("maximum concurrent ")
}
