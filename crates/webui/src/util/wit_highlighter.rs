use std::path::PathBuf;

use anyhow::Context;
use hashbrown::HashSet;
use wit_component::{Output, WitPrinter};
use wit_parser::{Resolve, Type, TypeDefKind, TypeOwner, UnresolvedPackageGroup};

use crate::grpc::{ifc_fqn::IfcFqn, pkg_fqn::PkgFqn};

pub fn print_all(wit: &str) -> String {
    let group = UnresolvedPackageGroup::parse(PathBuf::new(), wit).expect("FIXME");
    let mut resolve = Resolve::new();
    let main_id = resolve.push_group(group).expect("FIXME");
    let ids = resolve
        .packages
        .iter()
        .map(|(id, _)| id)
        // The main package would show as a nested package as well
        .filter(|id| *id != main_id)
        .collect::<Vec<_>>();
    let output_to_html = WitPrinter::<OutputToHtml>::new()
        .print_all(&resolve, main_id, &ids)
        .expect("FIXME");
    output_to_html.into()
}

pub fn print_interface(wit: &str, ifc_fqn: &IfcFqn) -> Result<String, anyhow::Error> {
    let group = UnresolvedPackageGroup::parse(PathBuf::new(), wit).expect("FIXME");
    let mut resolve = Resolve::new();
    let _main_id = resolve.push_group(group).expect("FIXME");
    let mut printer = WitPrinter::<OutputToHtml>::new();

    print_interface_with_imported_types(
        &mut printer,
        &resolve,
        ifc_fqn,
        &mut HashSet::new(),
        true,
    )?;
    Ok(printer.output.into())
}

fn print_interface_with_imported_types(
    printer: &mut WitPrinter<OutputToHtml>,
    resolve: &Resolve,
    ifc_fqn: &IfcFqn,
    additional_ifc_fqn: &mut HashSet<IfcFqn>,
    is_root_package: bool,
) -> Result<(), anyhow::Error> {
    if let Some((_pkg_id, package)) = &resolve
        .packages
        .iter()
        .find(|(_, package)| PkgFqn::from(&package.name) == ifc_fqn.pkg_fqn)
    {
        if !is_root_package {
            printer.output.newline();
        }

        printer
            .print_package_outer(package)
            .with_context(|| format!("error in `print_package_line` when printing {ifc_fqn}"))?;
        if is_root_package {
            printer.output.semicolon();
            printer.output.newline();
        } else {
            printer.output.indent_start();
        }
        let (ifc_name, ifc_id) = package
            .interfaces
            .iter()
            .find(|(name, _id)| ifc_fqn.ifc_name == **name)
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
                &Default::default(), // ignore resource funcs
            )?;
        }

        printer.output.indent_end();

        if !is_root_package {
            printer.output.indent_end();
        }

        // Look up imported types and print their intefaces recursively,

        let requested_pkg_owner = TypeOwner::Interface(ifc_id);
        for (_name, ty_id) in interface.types.iter() {
            let ty = &resolve.types[*ty_id];
            if let TypeDefKind::Type(Type::Id(other)) = ty.kind {
                let other = &resolve.types[other];

                if requested_pkg_owner != other.owner {
                    let ifc_id = match other.owner {
                        TypeOwner::Interface(id) => id,
                        // it's only possible to import types from interfaces at
                        // this time.
                        _ => unreachable!(),
                    };
                    let iface = &resolve.interfaces[ifc_id];
                    if let Some(imported_pkg_id) = iface.package {
                        let imported_pkg = &resolve.packages[imported_pkg_id];
                        if let Ok(ifc_fqn) = PkgFqn::from(&imported_pkg.name).ifc_fqn(iface) {
                            if additional_ifc_fqn.insert(ifc_fqn.clone()) {
                                print_interface_with_imported_types(
                                    printer,
                                    resolve,
                                    &ifc_fqn,
                                    additional_ifc_fqn,
                                    false,
                                )?;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[derive(Default)]
pub struct OutputToHtml {
    indent: usize,
    output: String,
    // set to true after newline, then to false after first item is indented.
    needs_indent: bool,
}

impl OutputToHtml {
    fn indent_if_needed(&mut self) {
        if self.needs_indent {
            for _ in 0..self.indent {
                // Indenting by two spaces.
                self.output.push_str("  ");
            }
            self.needs_indent = false;
        }
    }

    fn indent_and_print_escaped(&mut self, src: &str) {
        self.indent_if_needed();
        html_escape::encode_text_to_string(src, &mut self.output);
    }

    fn indent_and_print_in_span(&mut self, src: &str, class: &str) {
        self.indent_if_needed();
        self.output.push_str("<span class=\"");
        self.output.push_str(class);
        self.output.push_str("\">");
        html_escape::encode_text_to_string(src, &mut self.output);
        self.output.push_str("</span>");
    }
}

impl Output for OutputToHtml {
    fn newline(&mut self) {
        self.output.push('\n');
        self.needs_indent = true;
    }

    fn keyword(&mut self, src: &str) {
        self.indent_and_print_in_span(src, "keyword");
    }

    fn r#type(&mut self, src: &str) {
        self.indent_and_print_in_span(src, "type");
    }

    fn param(&mut self, src: &str) {
        self.indent_and_print_in_span(src, "param");
    }

    fn case(&mut self, src: &str) {
        self.indent_and_print_in_span(src, "case");
    }

    fn generic_args_start(&mut self) {
        html_escape::encode_text_to_string("<", &mut self.output);
    }

    fn generic_args_end(&mut self) {
        html_escape::encode_text_to_string(">", &mut self.output);
    }

    fn doc(&mut self, doc: &str) {
        assert!(!doc.contains('\n'));
        self.indent_if_needed();
        self.output.push_str("///");
        if !doc.is_empty() {
            self.output.push(' ');
            html_escape::encode_text_to_string(doc, &mut self.output);
        }
        self.newline();
    }

    fn version(&mut self, src: &str, at_sign: bool) {
        if at_sign {
            self.output.push('@');
        }
        self.indent_and_print_in_span(src, "version");
    }

    fn semicolon(&mut self) {
        assert!(
            !self.needs_indent,
            "`semicolon` is never called after newline"
        );
        self.output.push(';');
        self.newline();
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
        // Note that a `saturating_sub` is used here to prevent a panic
        // here in the case of invalid code being generated in debug
        // mode. It's typically easier to debug those issues through
        // looking at the source code rather than getting a panic.
        self.indent = self.indent.saturating_sub(1);
        self.indent_if_needed();
        self.output.push('}');
        self.newline();
    }

    fn str(&mut self, src: &str) {
        self.indent_and_print_escaped(src);
    }
}

impl From<OutputToHtml> for String {
    fn from(value: OutputToHtml) -> Self {
        value.output
    }
}
