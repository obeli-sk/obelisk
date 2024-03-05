use concepts::{FnName, FunctionFqn, FunctionMetadata, IfcFqnName};
use std::{collections::HashMap, error::Error};
use tracing_unwrap::OptionExt;
use val_json::{TypeWrapper, UnsupportedTypeError};
use wit_component::DecodedWasm;
use wit_parser::{Resolve, WorldId, WorldItem, WorldKey};

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error(transparent)]
    ParseError(Box<dyn Error>),
    #[error("wasm is not a component")]
    NotAComponent,
    #[error("empty packages are not supported")]
    EmptyPackage,
    #[error("empty interfaces are not supported")]
    EmptyInterface,
}

pub fn decode(wasm: &[u8]) -> Result<(Resolve, WorldId), DecodeError> {
    let decoded = wit_component::decode(wasm).map_err(|err| DecodeError::ParseError(err.into()))?;
    match decoded {
        DecodedWasm::Component(resolve, world_id) => Ok((resolve, world_id)),
        DecodedWasm::WitPackage(..) => Err(DecodeError::NotAComponent),
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
) -> Result<Vec<PackageIfcFns<'a>>, DecodeError> {
    let world = resolve
        .worlds
        .get(*world_id)
        .expect_or_log("world must exist");
    ifc_fns(resolve, world.exports.iter())
}

pub fn imported_ifc_fns<'a>(
    resolve: &'a Resolve,
    world_id: &'a WorldId,
) -> Result<Vec<PackageIfcFns<'a>>, DecodeError> {
    let world = resolve
        .worlds
        .get(*world_id)
        .expect_or_log("world must exist");
    ifc_fns(resolve, world.imports.iter())
}

fn ifc_fns<'a>(
    resolve: &'a Resolve,
    iter: impl Iterator<Item = (&'a WorldKey, &'a WorldItem)>,
) -> Result<Vec<PackageIfcFns<'a>>, DecodeError> {
    iter.filter_map(|(_, item)| match item {
        wit_parser::WorldItem::Interface(ifc) => {
            let ifc = resolve.interfaces.get(*ifc).unwrap_or_log();
            let Some(package_name) = ifc
                .package
                .and_then(|pkg| resolve.packages.get(pkg))
                .map(|p| &p.name)
            else {
                return Some(Err(DecodeError::EmptyPackage));
            };
            let Some(ifc_name) = ifc.name.as_deref() else {
                return Some(Err(DecodeError::EmptyInterface));
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

#[derive(thiserror::Error, Debug)]
pub enum FunctionMetadataError {
    #[error("{0}")]
    UnsupportedType(#[from] UnsupportedTypeError),

    #[error("unsupported return type in {fqn}, got type `{ty}`")]
    UnsupportedReturnType { fqn: String, ty: String },
}

pub fn functions_to_metadata(
    exported_interfaces: Vec<PackageIfcFns>,
) -> Result<HashMap<FunctionFqn, FunctionMetadata>, FunctionMetadataError> {
    let mut functions_to_results = HashMap::new();
    for PackageIfcFns {
        package_name,
        ifc_name,
        fns,
    } in exported_interfaces
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
        for (function_name, function) in fns {
            let fqn = FunctionFqn::new(ifc_fqn.clone(), function_name.clone());
            let params = function
                .params
                .iter()
                .map(|(name, ty)| TypeWrapper::try_from(*ty).map(|ty| (name.clone(), ty)))
                .collect::<Result<_, UnsupportedTypeError>>()?;
            match &function.results {
                wit_parser::Results::Anon(_) => Ok(()),
                wit_parser::Results::Named(named) if named.is_empty() => Ok(()),
                other @ wit_parser::Results::Named(_) => {
                    Err(FunctionMetadataError::UnsupportedReturnType {
                        fqn: fqn.to_string(),
                        ty: format!("{other:?}"),
                    })
                }
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

pub fn is_limit_reached(reason: &str) -> bool {
    // FIXME: use let trap = err.downcast::<Trap>().unwrap();
    reason.starts_with("maximum concurrent ")
}

#[cfg(test)]
mod tests {
    use tracing_unwrap::ResultExt;

    #[test]
    fn test_exported_interfaces() {
        let wasm_path = test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW;
        let wasm = std::fs::read(wasm_path).unwrap_or_log();
        let (resolve, world_id) = super::decode(&wasm).unwrap_or_log();
        let exported_interfaces = super::exported_ifc_fns(&resolve, &world_id).unwrap_or_log();
        println!(" {exported_interfaces:#?}",);
    }

    #[test]
    fn test_imported_functions() {
        let wasm_path = test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW;
        let wasm = std::fs::read(wasm_path).unwrap_or_log();
        let (resolve, world_id) = super::decode(&wasm).unwrap_or_log();
        let exported_interfaces = super::imported_ifc_fns(&resolve, &world_id).unwrap_or_log();
        println!(" {exported_interfaces:#?}");
        assert_eq!(exported_interfaces.len(), 2);
    }
}
