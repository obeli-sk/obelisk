use crate::{
    components::ffqn_with_links::FfqnWithLinks,
    grpc::{ffqn::FunctionFqn, ifc_fqn::IfcFqn, pkg_fqn::PkgFqn},
};
use anyhow::Context;
use hashbrown::HashSet;
use std::path::PathBuf;
use wit_component::{Output, TypeKind, WitPrinterExt};
use wit_parser::{Resolve, Type, TypeDefKind, TypeOwner, UnresolvedPackageGroup};
use yew::{html, Html, ToHtml};

pub fn print_all(
    wit: &str,
    render_ffqn_with_links: HashSet<FunctionFqn>,
) -> Result<Vec<Html>, anyhow::Error> {
    let group = UnresolvedPackageGroup::parse(PathBuf::new(), wit)?;
    let mut resolve = Resolve::new();
    let main_id = resolve.push_group(group)?;
    let ids = resolve
        .packages
        .iter()
        .map(|(id, _)| id)
        // The main package would show as a nested package as well
        .filter(|id| *id != main_id)
        .collect::<Vec<_>>();
    let mut printer = WitPrinterExt::new(OutputToHtml::default());
    printer.output.render_ffqn_with_links = render_ffqn_with_links;
    let output_to_html = printer.print_all(&resolve, main_id, &ids)?;
    Ok(output_to_html.output)
}

pub fn print_interface_with_single_fn(
    wit: &str,
    ffqn: &FunctionFqn,
) -> Result<Vec<Html>, anyhow::Error> {
    let group = UnresolvedPackageGroup::parse(PathBuf::new(), wit)?;
    let mut resolve = Resolve::new();
    let _main_id = resolve.push_group(group)?;
    let mut printer = WitPrinterExt::new(OutputToHtml::default());
    printer.output.filter = PrintFilter::SubmitPage(ffqn.clone());

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
    printer: &mut WitPrinterExt<OutputToHtml>,
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
enum PrintFilter {
    #[default]
    ShowAll,
    SubmitPage(FunctionFqn),
}

#[derive(Default)]
pub struct OutputToHtml {
    indent: usize,
    output: Vec<Html>,
    // set to true after newline, then to false after first item is indented.
    needs_indent: bool,
    filter: PrintFilter,
    ignore_until_end_of_line: usize,
    render_ffqn_with_links: HashSet<FunctionFqn>,
    current_namespace: Option<String>,
    current_package_name: Option<String>,
    current_version: Option<String>,
    current_interface: Option<String>,
}

impl OutputToHtml {
    fn push(&mut self, src: char) {
        if self.ignore_until_end_of_line == 0 {
            self.output.push(src.to_html());
        }
    }

    fn push_str(&mut self, src: &str) {
        assert!(!src.contains('\n'));
        if self.ignore_until_end_of_line == 0 {
            self.output.push(src.to_html());
        }
    }

    fn push_html(&mut self, html: Html) {
        if self.ignore_until_end_of_line == 0 {
            self.output.push(html);
        }
    }

    fn indent_if_needed(&mut self) {
        if self.ignore_until_end_of_line == 0 && self.needs_indent {
            for _ in 0..self.indent {
                // Indenting by two spaces.
                self.push_str("  ");
            }
            self.needs_indent = false;
        }
    }

    fn indent_and_print(&mut self, src: &str) {
        self.indent_if_needed();
        self.push_str(src);
    }

    fn indent_and_print_in_span(&mut self, src: &str, class: &'static str) {
        self.indent_if_needed();
        self.push_html(html! {
            <span class={class}>{src}</span>
        });
    }
}

impl Output for OutputToHtml {
    fn newline(&mut self) {
        self.push('\n'); // ignore when muted
        self.needs_indent = true;
        self.ignore_until_end_of_line = self.ignore_until_end_of_line.saturating_sub(1);
    }

    fn keyword(&mut self, src: &str) {
        self.indent_and_print_in_span(src, "keyword");
    }

    fn r#type(&mut self, src: &str, kind: TypeKind) {
        match kind {
            TypeKind::NamespaceDeclaration => {
                self.current_namespace = Some(src.to_string());
                self.current_package_name = None;
                self.current_version = None;
                self.current_interface = None;
            }
            TypeKind::PackageNameDeclaration => {
                self.current_package_name = Some(src.to_string());
                self.current_version = None;
                self.current_interface = None;
            }
            TypeKind::InterfaceDeclaration => {
                self.current_interface = Some(src.to_string());
            }
            _ => {}
        }

        let css_class = match kind {
            TypeKind::FunctionFreestanding
            | TypeKind::FunctionMethod
            | TypeKind::FunctionStatic => {
                if matches!(&self.filter, PrintFilter::SubmitPage(ffqn) if ffqn.function_name != src)
                {
                    self.ignore_until_end_of_line = 2; // after the function is printed an empty line is added.
                }
                "func"
            }
            TypeKind::VersionDeclaration | TypeKind::VersionPath | TypeKind::VersionAnnotation => {
                "version"
            }
            _ => "type",
        };

        if let (Some(ns), Some(p), v, Some(ifc), TypeKind::FunctionFreestanding) = (
            &self.current_namespace,
            &self.current_package_name,
            &self.current_version,
            &self.current_interface,
            &kind,
        ) {
            let ffqn = FunctionFqn {
                ifc_fqn: IfcFqn {
                    pkg_fqn: PkgFqn {
                        namespace: ns.clone(),
                        package_name: p.clone(),
                        version: v.clone(),
                    },
                    ifc_name: ifc.clone(),
                },
                function_name: src.to_string(),
            };
            if self.render_ffqn_with_links.contains(&ffqn) {
                self.indent_if_needed();
                self.push_html(html! {<>
                    <span class={"func"}>
                        <FfqnWithLinks {ffqn} />
                    </span>
                </>});
                return;
            }
        }

        self.indent_and_print_in_span(src, css_class);
    }

    fn param(&mut self, src: &str) {
        self.indent_and_print_in_span(src, "param");
    }

    fn case(&mut self, src: &str) {
        self.indent_and_print_in_span(src, "case");
    }

    fn generic_args_start(&mut self) {
        self.push_str("<");
    }

    fn generic_args_end(&mut self) {
        self.push_str(">");
    }

    fn doc(&mut self, src: &str) {
        assert!(!src.contains('\n'));
        self.indent_if_needed();
        self.push_str("///");
        if !src.is_empty() {
            self.push(' ');
            self.push_str(src);
        }
        self.newline();
    }

    fn semicolon(&mut self) {
        assert!(
            self.ignore_until_end_of_line > 0 || !self.needs_indent,
            "`semicolon` is never called after newline"
        );
        self.push(';');
        self.newline();
    }

    fn indent_start(&mut self) {
        assert!(
            self.ignore_until_end_of_line > 0 || !self.needs_indent,
            "`indent_start` is never called after newline"
        );
        self.push_str(" {");
        self.indent += 1;
        self.newline();
    }

    fn indent_end(&mut self) {
        self.ignore_until_end_of_line = 0;
        // Note that a `saturating_sub` is used here to prevent a panic
        // here in the case of invalid code being generated in debug
        // mode. It's typically easier to debug those issues through
        // looking at the source code rather than getting a panic.
        self.indent = self.indent.saturating_sub(1);
        self.indent_if_needed();
        self.push('}');
        self.newline();
    }

    fn str(&mut self, src: &str) {
        self.indent_and_print(src);
    }
}
