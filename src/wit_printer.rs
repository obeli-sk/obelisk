//! WIT printing utilities using `wit_component::WitPrinter`.

use anyhow::{Context, bail};
use concepts::{FunctionFqn, IfcFqnName, PkgFqn};
use hashbrown::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::{debug, info};
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
pub fn print_interface_with_single_fn(wit: &str, ffqn: &FunctionFqn) -> Result<String, anyhow::Error> {
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
    if let Some((_pkg_id, package)) = resolve.packages.iter().find(|(_, package)| {
        package_name_to_pkg_fqn(&package.name) == ifc_fqn.pkg_fqn_name()
    }) {
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
                interface.types.iter().map(|(name, id)| (name.as_str(), *id)),
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
                            imported_pkg.name.version.as_ref().map(|v| v.to_string()).as_deref(),
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
                self.output.push_str("  ");
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
        if let Some(ref filter_fn) = self.filter_fn_name {
            if matches!(
                kind,
                TypeKind::FunctionFreestanding | TypeKind::FunctionMethod | TypeKind::FunctionStatic
            ) && src != filter_fn
            {
                self.ignore_until_end_of_line = 2;
            }
        }
        self.indent_if_needed();
        self.push_str(src);
    }

    fn semicolon(&mut self) {
        self.push_str(";");
        self.newline();
    }
}

/// Process a package and its dependencies, writing to files.
pub async fn process_pkg_with_deps(
    wit: &str,
    pkg: &PkgFqn,
    already_processed_packages: &mut HashSet<PkgFqn>,
    output: OutputToFile,
) -> Result<OutputToFile, anyhow::Error> {
    let group = UnresolvedPackageGroup::parse(PathBuf::new(), wit)?;
    let mut resolve = Resolve::new();
    let _main_id = resolve.push_group(group)?;
    let mut printer = WitPrinter::new(output);

    process_pkg_inner(&mut printer, &resolve, pkg, already_processed_packages)?;
    Ok(printer.output)
}

fn process_pkg_inner(
    printer: &mut WitPrinter<OutputToFile>,
    resolve: &Resolve,
    pkg: &PkgFqn,
    already_processed_packages: &mut HashSet<PkgFqn>,
) -> Result<(), anyhow::Error> {
    if let Some(package) = resolve.packages.iter().find_map(|(_, package)| {
        let pkg_fqn = package_name_to_pkg_fqn(&package.name);
        if pkg_fqn == *pkg {
            Some(package)
        } else {
            None
        }
    }) {
        {
            let is_new = already_processed_packages.insert(pkg.clone());
            if !is_new {
                return Ok(());
            }
        }
        printer.output.change_current_pkg(pkg.clone());

        printer
            .print_package_outer(package)
            .with_context(|| format!("cannot print {pkg}"))?;

        printer.output.semicolon();
        printer.output.newline();

        let mut dependent_packages = HashSet::new();

        for (ifc_name, ifc_id) in &package.interfaces {
            let ifc_id = *ifc_id;
            printer
                .print_interface_outer(resolve, ifc_id, ifc_name)
                .with_context(|| format!("cannot print {ifc_name}"))?;
            printer.output.indent_start();
            let interface = &resolve.interfaces[ifc_id];
            printer.print_interface(resolve, ifc_id)?;
            printer.output.indent_end();

            // Look up imported types and print their packages recursively,
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
                            && let imported_pkg = &resolve.packages[imported_pkg_id]
                            && let pkg_fqn = package_name_to_pkg_fqn(&imported_pkg.name)
                            && !already_processed_packages.contains(&pkg_fqn)
                        {
                            dependent_packages.insert(pkg_fqn);
                        }
                    }
                }
            }
        }
        printer.output.finish_pkg();

        for next_pkg in dependent_packages {
            process_pkg_inner(printer, resolve, &next_pkg, already_processed_packages)?;
        }
    }
    Ok(())
}

/// Output implementation that writes to multiple files organized by package.
#[derive(Default)]
pub struct OutputToFile {
    current_pkg: Option<PkgFqn>,
    indent: usize,
    output: String,
    needs_indent: bool,
    all_pkgs: HashMap<PkgFqn, String>,
}

impl OutputToFile {
    fn change_current_pkg(&mut self, pkg_fqn: PkgFqn) {
        debug!("Setting current package to {pkg_fqn}");
        assert!(
            self.current_pkg.is_none(),
            "last package was not finished properly"
        );
        self.current_pkg = Some(pkg_fqn);
    }

    fn finish_pkg(&mut self) {
        if let Some(old_pkg) = self.current_pkg.take() {
            debug!("Finishing package {old_pkg}");
            let output = std::mem::take(&mut self.output);
            let conflict = self.all_pkgs.insert(old_pkg, output.clone());
            if let Some(conflict) = conflict
                && conflict != output
            {
                println!("Conflict: old: {conflict}");
                println!("Conflict: new: {output}");
            }
            self.indent = 0;
            self.needs_indent = false;
        }
    }

    pub async fn write(self, output_directory: &Path, overwrite: bool) -> Result<(), anyhow::Error> {
        for (pkg_fqn, contents) in self.all_pkgs {
            let pkg_file_name = pkg_fqn.as_file_name();
            let directory = output_directory.join(&pkg_file_name);
            tokio::fs::create_dir_all(&directory)
                .await
                .with_context(|| format!("cannot create the output directory {directory:?}"))?;

            let target_wit = directory.join(format!("{pkg_file_name}.wit"));
            info!("Writing {target_wit:?}");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .create_new(!overwrite)
                .open(&target_wit)
                .await
                .with_context(|| {
                    format!(
                        "cannot open {target_wit:?} for writing{}",
                        if !overwrite {
                            ", try using `--overwrite`"
                        } else {
                            ""
                        }
                    )
                })?;

            file.write_all(contents.as_bytes())
                .await
                .with_context(|| format!("cannot write to {target_wit:?}"))?;
        }
        Ok(())
    }
}

impl Output for OutputToFile {
    fn push_str(&mut self, src: &str) {
        self.output.push_str(src);
    }

    fn indent_if_needed(&mut self) -> bool {
        if self.needs_indent {
            for _ in 0..self.indent {
                self.output.push_str("  ");
            }
            self.needs_indent = false;
            true
        } else {
            false
        }
    }

    fn indent_start(&mut self) {
        assert!(
            !self.needs_indent,
            "`indent_start` is never called after newline"
        );
        self.output.push_str(" {");
        self.indent += 1;
        self.newline();
    }

    fn indent_end(&mut self) {
        self.indent = self.indent.saturating_sub(1);
        self.indent_if_needed();
        self.output.push('}');
        self.newline();
    }

    fn newline(&mut self) {
        self.output.push('\n');
        self.needs_indent = true;
    }
}
