use crate::{
    components::ffqn_with_links::FfqnWithLinks,
    grpc::{
        ffqn::FunctionFqn,
        function_detail::{map_interfaces_to_fn_details, InterfaceFilter},
        grpc_client::{self, FunctionDetail},
        ifc_fqn::IfcFqn,
        pkg_fqn::PkgFqn,
        SUFFIX_PKG_EXT,
    },
};
use anyhow::Context;
use hashbrown::{HashMap, HashSet};
use id_arena::Arena;
use std::{path::PathBuf, str::FromStr};
use wit_component::{Output, TypeKind, WitPrinterExt};
use wit_parser::{
    Function, FunctionKind, Handle, Interface, InterfaceId, PackageName, Resolve, Results, Type,
    TypeDef, TypeDefKind, TypeOwner, UnresolvedPackageGroup,
};
use yew::{html, Html, ToHtml};

fn find_interface<'a>(
    ifc_fqn: IfcFqn,
    resolve: &'_ Resolve,
    interfaces: &'a Arena<Interface>,
) -> Option<(InterfaceId, &'a Interface)> {
    let pkg_id = *resolve
        .package_names
        .get(&PackageName::try_from(ifc_fqn.pkg_fqn).ok()?)?;
    let ifc_id = *resolve.packages[pkg_id].interfaces.get(&ifc_fqn.ifc_name)?;
    interfaces.get(ifc_id).map(|ifc| (ifc_id, ifc))
}

pub fn print_all(
    wit: &str,
    render_ffqn_with_links: HashSet<FunctionFqn>,
    component: &grpc_client::Component,
) -> Result<Vec<Html>, anyhow::Error> {
    let group = UnresolvedPackageGroup::parse(PathBuf::new(), wit)?;
    let mut resolve = Resolve::new();
    let main_id = resolve.push_group(group)?;

    let mut exported_pkg_to_ifc_to_details_map: HashMap<
        PkgFqn,
        HashMap<IfcFqn, Vec<FunctionDetail>>,
    > = HashMap::new();
    for (exported_ifc, fn_details) in
        map_interfaces_to_fn_details(&component.exports, InterfaceFilter::default())
    {
        if !exported_ifc.pkg_fqn.is_extension() {
            let ifc_to_details = exported_pkg_to_ifc_to_details_map
                .entry(exported_ifc.pkg_fqn.clone())
                .or_default();
            ifc_to_details.insert(exported_ifc, fn_details);
        }
    }

    // Find necessary handles
    let (execution_ifc_id, execution_ifc) = find_interface(
        IfcFqn::from_str("obelisk:types/execution").unwrap(),
        &resolve,
        &resolve.interfaces,
    )
    .expect("TODO");
    let (time_ifc_id, time_ifc) = find_interface(
        IfcFqn::from_str("obelisk:types/time").unwrap(),
        &resolve,
        &resolve.interfaces,
    )
    .expect("TODO");
    let type_id_execution_id = {
        // obelisk:types/execution.{execution-id}
        let actual_type_id = *execution_ifc.types.get("execution-id").expect("TODO4");
        // Create a reference to the type.
        resolve.types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Type(Type::Id(actual_type_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: Default::default(),
            stability: Default::default(),
        })
    };
    let (type_id_join_set_id, type_id_join_set_id_borrow_handle) = {
        // obelisk:types/execution.{join-set-id}
        let actual_type_id = *execution_ifc.types.get("join-set-id").expect("TODO4");
        // Create a reference to the type.
        let type_id_join_set_id = resolve.types.alloc(TypeDef {
            name: Some("join-set-id".to_string()),
            kind: TypeDefKind::Type(Type::Id(actual_type_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: Default::default(),
            stability: Default::default(),
        });
        // Create a Handle::Borrow to the reference.
        let type_id_join_set_id_borrow_handle = resolve.types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Handle(Handle::Borrow(type_id_join_set_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: Default::default(),
            stability: Default::default(),
        });
        (type_id_join_set_id, type_id_join_set_id_borrow_handle)
    };
    let type_id_schedule_at = {
        // obelisk:types/time.{schedule-at}
        let actual_type_id = *time_ifc.types.get("schedule-at").expect("TODO-schedule-at");
        // Create a reference to the type.
        resolve.types.alloc(TypeDef {
            name: None,
            kind: TypeDefKind::Type(Type::Id(actual_type_id)),
            owner: TypeOwner::Interface(execution_ifc_id),
            docs: Default::default(),
            stability: Default::default(),
        })
    };

    for (pkg_fqn, ifc_to_details) in exported_pkg_to_ifc_to_details_map {
        let pkg_ext_fqn = PkgFqn {
            namespace: pkg_fqn.namespace.clone(),
            package_name: format!("{}{SUFFIX_PKG_EXT}", pkg_fqn.package_name),
            version: pkg_fqn.version.clone(),
        };

        // Get or create the -obelisk-ext variant of the exported package.
        let ext_pkg_id = match resolve
            .packages
            .iter()
            .find(|(_, found_pkg)| PkgFqn::from(&found_pkg.name) == pkg_ext_fqn)
        {
            Some((pkg_ext_id, _)) => pkg_ext_id,
            None => {
                let pkg_ext = wit_parser::Package {
                    name: pkg_ext_fqn.try_into()?,
                    docs: Default::default(),
                    interfaces: Default::default(),
                    worlds: Default::default(),
                };
                let package_name = pkg_ext.name.clone();
                let ext_pkg_id = resolve.packages.alloc(pkg_ext);
                resolve.package_names.insert(package_name, ext_pkg_id);
                ext_pkg_id
            }
        };

        for (ifc_fqn, fn_details) in ifc_to_details {
            let (_, original_ifc) =
                find_interface(ifc_fqn.clone(), &resolve, &resolve.interfaces).expect("TODO");
            let mut types = original_ifc.types.clone();
            types.insert("execution-id".to_string(), type_id_execution_id);
            types.insert("join-set-id".to_string(), type_id_join_set_id);
            types.insert("schedule-at".to_string(), type_id_schedule_at);

            let mut ext_ifc = Interface {
                name: Some(ifc_fqn.ifc_name.clone()),
                types,
                functions: Default::default(),
                docs: Default::default(),
                stability: Default::default(),
                package: Some(ext_pkg_id),
            };
            for fn_detail in fn_details {
                let ffqn = FunctionFqn::from_fn_detail(&fn_detail).expect("TODO");
                let original_fn = original_ifc
                    .functions
                    .get(&ffqn.function_name)
                    .expect("TODO");
                // -submit: func(join-set-id: borrow<join-set-id>, <params>) -> execution-id;
                {
                    let fn_name = format!("{}-submit", ffqn.function_name);
                    let mut params = vec![(
                        "join-set-id".to_string(),
                        Type::Id(type_id_join_set_id_borrow_handle),
                    )];
                    params.extend_from_slice(&original_fn.params);
                    let fn_ext = Function {
                        name: fn_name.clone(),
                        kind: FunctionKind::Freestanding,
                        params,
                        results: Results::Anon(Type::Id(type_id_execution_id)),
                        docs: Default::default(),
                        stability: Default::default(),
                    };
                    ext_ifc.functions.insert(fn_name, fn_ext);
                }
                // -await-next: func(join-set-id: borrow<join-set-id>) -> result<tuple<execution-id, <return-type>>, tuple<execution-id, execution-error>>;

                // -schedule  -schedule: func(schedule-at: schedule-at, <params>) -> execution-id;
                {
                    let fn_name = format!("{}-schedule", ffqn.function_name);
                    let mut params =
                        vec![("schedule-at".to_string(), Type::Id(type_id_schedule_at))];
                    params.extend_from_slice(&original_fn.params);
                    let fn_ext = Function {
                        name: fn_name.clone(),
                        kind: FunctionKind::Freestanding,
                        params,
                        results: Results::Anon(Type::Id(type_id_execution_id)),
                        docs: Default::default(),
                        stability: Default::default(),
                    };
                    ext_ifc.functions.insert(fn_name, fn_ext);
                }
            }
            let ext_ifc_id = resolve.interfaces.alloc(ext_ifc);
            resolve
                .packages
                .get_mut(ext_pkg_id)
                .expect("found or inserted already")
                .interfaces
                .insert(ifc_fqn.ifc_name, ext_ifc_id);
        }
    }

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

    fn r#type(&mut self, mut src: &str, kind: TypeKind) {
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
            TypeKind::VersionDeclaration => {
                self.current_version = Some(src.to_string());
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
                if let Some(suffix) = src.strip_prefix('@') {
                    self.indent_and_print("@");
                    src = suffix;
                }
                "version"
            }
            _ => "type",
        };

        if let (
            Some(namespace),
            Some(package_name),
            version,
            Some(ifc),
            TypeKind::FunctionFreestanding,
        ) = (
            &self.current_namespace,
            &self.current_package_name,
            &self.current_version,
            &self.current_interface,
            &kind,
        ) {
            let ffqn = FunctionFqn {
                ifc_fqn: IfcFqn {
                    pkg_fqn: PkgFqn {
                        namespace: namespace.clone(),
                        package_name: package_name.clone(),
                        version: version.clone(),
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
