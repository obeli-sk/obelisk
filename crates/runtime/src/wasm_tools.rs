use crate::{FnName, FunctionFqn, FunctionMetadata, FunctionMetadataError, IfcFqnName};
use anyhow::{anyhow, bail};
use std::collections::HashMap;
use val_json::{TypeWrapper, UnsupportedTypeError};
use wit_component::DecodedWasm;
use wit_parser::{Resolve, WorldId, WorldItem, WorldKey};

pub fn decode(wasm: &[u8]) -> Result<(Resolve, WorldId), anyhow::Error> {
    let decoded = wit_component::decode(wasm)?;
    match decoded {
        DecodedWasm::Component(resolve, world_id) => Ok((resolve, world_id)),
        _ => bail!("cannot parse component"),
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PackageIfcFns<'a> {
    package_name: &'a wit_parser::PackageName,
    ifc_name: &'a str,
    fns: &'a indexmap::IndexMap<String, wit_parser::Function>,
}

pub fn exported_ifc_fns<'a>(
    resolve: &'a Resolve,
    world_id: &'a WorldId,
) -> Result<Vec<PackageIfcFns<'a>>, anyhow::Error> {
    let world = resolve
        .worlds
        .get(*world_id)
        .ok_or_else(|| anyhow!("world must exist"))?;
    ifc_fns(resolve, world.exports.iter())
}

pub fn imported_ifc_fns<'a>(
    resolve: &'a Resolve,
    world_id: &'a WorldId,
) -> Result<Vec<PackageIfcFns<'a>>, anyhow::Error> {
    let world = resolve
        .worlds
        .get(*world_id)
        .ok_or_else(|| anyhow!("world must exist"))?;
    ifc_fns(resolve, world.imports.iter())
}

fn ifc_fns<'a>(
    resolve: &'a Resolve,
    iter: impl Iterator<Item = (&'a WorldKey, &'a WorldItem)>,
) -> Result<Vec<PackageIfcFns<'a>>, anyhow::Error> {
    iter.filter_map(|(_, item)| match item {
        wit_parser::WorldItem::Interface(ifc) => {
            let ifc = resolve.interfaces.get(*ifc).unwrap_or_else(|| panic!());
            let Some(package_name) = ifc
                .package
                .and_then(|pkg| resolve.packages.get(pkg))
                .map(|p| &p.name)
            else {
                return Some(Err(anyhow!("empty packages are not supported")));
            };
            let Some(ifc_name) = ifc.name.as_deref() else {
                return Some(Err(anyhow!("empty interfaces are not supported")));
            };
            Some(Ok(PackageIfcFns {
                package_name,
                ifc_name,
                fns: &ifc.functions,
            }))
        }
        _ => None,
    })
    .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn functions_to_metadata(
    exported_interfaces: Vec<PackageIfcFns>,
) -> Result<HashMap<FunctionFqn, FunctionMetadata>, FunctionMetadataError> {
    let mut functions_to_results = HashMap::new();
    for PackageIfcFns {
        package_name,
        ifc_name,
        fns,
    } in exported_interfaces.into_iter()
    {
        let ifc_fqn = if let Some(version) = &package_name.version {
            format!(
                "{namespace}:{name}/{ifc_name}@{version}",
                namespace = package_name.namespace,
                name = package_name.name
            )
        } else {
            format!("{package_name}/{ifc_name}")
        };
        for (function_name, function) in fns.into_iter() {
            let fqn = FunctionFqn::new(ifc_fqn.clone(), function_name.clone());
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

pub fn group_by_ifc_to_fn_names<'a>(
    fqns: impl Iterator<Item = &'a FunctionFqn>,
) -> HashMap<IfcFqnName, Vec<FnName>> {
    let mut ifcs_to_fn_names = HashMap::<_, Vec<_>>::new();
    for fqn in fqns {
        ifcs_to_fn_names
            .entry(fqn.ifc_fqn.clone())
            .or_default()
            .push(fqn.function_name.clone());
    }
    ifcs_to_fn_names
}

pub(crate) fn is_limit_reached(reason: &str) -> bool {
    reason.starts_with("maximum concurrent ")
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_exported_interfaces() -> Result<(), anyhow::Error> {
        let wasm_path = test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW;
        let wasm = std::fs::read(wasm_path)?;
        let (resolve, world_id) = super::decode(&wasm)?;
        let exported_interfaces = super::exported_ifc_fns(&resolve, &world_id)?;
        println!(" {:#?}", exported_interfaces);
        Ok(())
    }

    #[tokio::test]
    async fn test_imported_functions() -> Result<(), anyhow::Error> {
        let wasm_path = test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW;
        let wasm = std::fs::read(wasm_path)?;
        let (resolve, world_id) = super::decode(&wasm)?;
        let exported_interfaces = super::imported_ifc_fns(&resolve, &world_id)?;
        println!(" {:#?}", exported_interfaces);
        assert_eq!(exported_interfaces.len(), 2);
        Ok(())
    }
}
