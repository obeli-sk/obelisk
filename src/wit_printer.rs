//! WIT printing utilities using `wit_component::WitPrinter`.

use anyhow::{Context as _, bail};
use concepts::{FunctionFqn, IfcFqnName, PkgFqn};
use hashbrown::HashSet;
use std::path::PathBuf;
use wit_component::{Output, TypeKind, WitPrinter};
use wit_parser::{PackageName, Resolve, Type, TypeDefKind, TypeOwner, UnresolvedPackageGroup};

/// Convert `wit_parser::PackageName` to `concepts::PkgFqn`.
pub fn package_name_to_pkg_fqn(value: &PackageName) -> PkgFqn {
    PkgFqn {
        namespace: value.namespace.clone(),
        package_name: value.name.clone(),
        version: value.version.as_ref().map(std::string::ToString::to_string),
    }
}

/// Print a single function and its types in WIT format.
pub fn print_interface_with_single_fn(
    wit: &str,
    ffqn: &FunctionFqn,
) -> Result<String, anyhow::Error> {
    let group = UnresolvedPackageGroup::parse(PathBuf::new(), wit)?;
    let mut resolve = Resolve::new();
    let _main_id = resolve.push_group(group)?;
    let mut printer = WitPrinter::new(OutputToString::new(Some(ffqn.function_name.to_string())));

    print_interface_with_imported_types(
        &mut printer,
        &resolve,
        &ffqn.ifc_fqn,
        &mut HashSet::new(),
        true,
    )?;
    Ok(printer.output.output)
}

fn print_interface_with_imported_types(
    printer: &mut WitPrinter<OutputToString>,
    resolve: &Resolve,
    ifc_fqn: &IfcFqnName,
    additional_ifc_fqn: &mut HashSet<IfcFqnName>,
    is_root_package: bool,
) -> Result<(), anyhow::Error> {
    if let Some((_pkg_id, package)) = resolve
        .packages
        .iter()
        .find(|(_, package)| package_name_to_pkg_fqn(&package.name) == ifc_fqn.pkg_fqn_name())
    {
        if !is_root_package {
            printer.output.newline();
        }

        printer.print_package_outer(package).with_context(|| {
            format!("error in `print_interface_with_imported_types` when printing {ifc_fqn}")
        })?;
        if is_root_package {
            printer.output.semicolon();
            printer.output.newline();
        } else {
            printer.output.indent_start();
        }
        let (ifc_name, ifc_id) = package
            .interfaces
            .iter()
            .find(|(name, _id)| ifc_fqn.ifc_name() == *name)
            .with_context(|| format!("interface not found - {ifc_fqn}"))?;

        let ifc_id = *ifc_id;
        printer
            .print_interface_outer(resolve, ifc_id, ifc_name)
            .with_context(|| format!("cannot print {ifc_name}"))?;
        printer.output.indent_start();
        let interface = &resolve.interfaces[ifc_id];
        if is_root_package {
            printer.print_interface(resolve, ifc_id)?;
        } else {
            // just print the types
            printer.print_types(
                resolve,
                TypeOwner::Interface(ifc_id),
                interface
                    .types
                    .iter()
                    .map(|(name, id)| (name.as_str(), *id)),
                &std::collections::HashMap::default(), // ignore resource funcs
            )?;
        }

        printer.output.indent_end();

        if !is_root_package {
            printer.output.indent_end();
        }

        // Look up imported types and print their interfaces recursively
        let requested_pkg_owner = TypeOwner::Interface(ifc_id);
        for (_name, ty_id) in &interface.types {
            let ty = &resolve.types[*ty_id];
            if let TypeDefKind::Type(Type::Id(other)) = ty.kind {
                let other = &resolve.types[other];

                if requested_pkg_owner != other.owner {
                    let ifc_id = match other.owner {
                        TypeOwner::Interface(id) => id,
                        other => bail!("unsupported type import from {other:?}"),
                    };
                    let iface = &resolve.interfaces[ifc_id];
                    if let Some(imported_pkg_id) = iface.package
                        && let Some(ifc_name) = &iface.name
                    {
                        let imported_pkg = &resolve.packages[imported_pkg_id];
                        let new_ifc_fqn = IfcFqnName::from_parts(
                            &imported_pkg.name.namespace,
                            &imported_pkg.name.name,
                            ifc_name,
                            imported_pkg
                                .name
                                .version
                                .as_ref()
                                .map(std::string::ToString::to_string)
                                .as_deref(),
                        );
                        if additional_ifc_fqn.insert(new_ifc_fqn.clone()) {
                            print_interface_with_imported_types(
                                printer,
                                resolve,
                                &new_ifc_fqn,
                                additional_ifc_fqn,
                                false,
                            )?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Output implementation that writes to a String, optionally filtering to a single function.
struct OutputToString {
    indent: usize,
    output: String,
    needs_indent: bool,
    filter_fn_name: Option<String>,
    ignore_until_end_of_line: usize,
}

impl OutputToString {
    fn new(filter_fn_name: Option<String>) -> Self {
        Self {
            indent: 0,
            output: String::new(),
            needs_indent: false,
            filter_fn_name,
            ignore_until_end_of_line: 0,
        }
    }
}

impl Output for OutputToString {
    fn push_str(&mut self, src: &str) {
        if self.ignore_until_end_of_line == 0 {
            self.output.push_str(src);
        }
    }

    fn indent_if_needed(&mut self) -> bool {
        if self.ignore_until_end_of_line == 0 && self.needs_indent {
            for _ in 0..self.indent {
                self.output.push_str("    ");
            }
            self.needs_indent = false;
            true
        } else {
            false
        }
    }

    fn indent_start(&mut self) {
        self.push_str(" {");
        self.indent += 1;
        self.newline();
    }

    fn indent_end(&mut self) {
        self.ignore_until_end_of_line = 0;
        self.indent = self.indent.saturating_sub(1);
        self.indent_if_needed();
        self.push_str("}");
        self.newline();
    }

    fn newline(&mut self) {
        self.push_str("\n");
        self.needs_indent = true;
        self.ignore_until_end_of_line = self.ignore_until_end_of_line.saturating_sub(1);
    }

    fn ty(&mut self, src: &str, kind: TypeKind) {
        if let Some(ref filter_fn) = self.filter_fn_name
            && matches!(
                kind,
                TypeKind::FunctionFreestanding
                    | TypeKind::FunctionMethod
                    | TypeKind::FunctionStatic
            )
            && src != filter_fn
        {
            self.ignore_until_end_of_line = 2;
        }
        self.indent_if_needed();
        self.push_str(src);
    }

    fn semicolon(&mut self) {
        self.push_str(";");
        self.newline();
    }
}
