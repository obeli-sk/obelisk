use crate::{
    app::AppState,
    components::{
        component_tree::{ComponentTree, ComponentTreeConfig},
        ffqn_with_links::FfqnWithLinks,
        function_signature::FunctionSignature,
    },
    grpc::{
        ffqn::FunctionFqn,
        function_detail::{map_interfaces_to_fn_details, InterfaceFilter},
        grpc_client::{self, ComponentType, FunctionDetail},
        ifc_fqn::IfcFqn,
    },
};
use indexmap::IndexMap;
use std::{ops::Deref, path::PathBuf};
use wit_component::{Output, WitPrinter};
use wit_parser::{Resolve, UnresolvedPackageGroup};
use yew::prelude::*;

#[function_component(ComponentListPage)]
pub fn component_list_page() -> Html {
    let app_state =
        use_context::<AppState>().expect("AppState context is set when starting the App");
    let components: Vec<_> = app_state.components;
    let components_with_idx = components
        .clone()
        .into_iter()
        .enumerate()
        .collect::<IndexMap<_, _>>();
    let selected_component_idx_state: UseStateHandle<Option<usize>> = use_state(|| None);
    let wit_state = use_state(|| None);
    // Fetch GetWit
    use_effect_with(*selected_component_idx_state.deref(), {
        let components_with_idx = components_with_idx.clone();
        let wit_state = wit_state.clone();
        move |selected_component_idx| {
            if let Some(selected_component_idx) = selected_component_idx {
                let component_id = components_with_idx
                    .get(selected_component_idx)
                    .expect("`selected_component_idx` must be valid")
                    .component_id
                    .clone()
                    .expect("`component_id` is sent");
                wasm_bindgen_futures::spawn_local(async move {
                    let base_url = "/api";
                    let mut fn_client =
                        grpc_client::function_repository_client::FunctionRepositoryClient::new(
                            tonic_web_wasm_client::Client::new(base_url.to_string()),
                        );
                    let wit = fn_client
                        .get_wit(grpc_client::GetWitRequest {
                            component_id: Some(component_id),
                        })
                        .await
                        .unwrap()
                        .into_inner()
                        .content;
                    wit_state.set(Some(wit));
                });
            } else {
                wit_state.set(None);
            }
        }
    });

    let component_detail = selected_component_idx_state
        .deref()
        .and_then(|idx| components.get(idx))
        .map(|component| {
            let component_type = ComponentType::try_from(component.r#type).unwrap();
            let exports =
                map_interfaces_to_fn_details(&component.exports, InterfaceFilter::WithExtensions);

             let render_ifc_with_fns = |ifc_fqn: &IfcFqn, fn_details: &[FunctionDetail] | {
                let submittable_fn_details = fn_details
                    .iter()
                    .filter(|fn_detail| fn_detail.submittable)
                    .map(|fn_detail| {
                        html! {
                            <li>
                                <FfqnWithLinks ffqn = {FunctionFqn::from_fn_detail(fn_detail)} />
                                {": "}
                                <span>
                                    <FunctionSignature params = {fn_detail.params.clone()} return_type={fn_detail.return_type.clone()} />
                                </span>
                            </li>
                        }
                    })
                    .collect::<Vec<_>>();

                html! {
                    <section class="types-interface">
                        <h3>
                            {format!("{}:{}/", ifc_fqn.namespace, ifc_fqn.package_name)}
                            <span class="highlight">
                                {&ifc_fqn.ifc_name}
                            </span>
                            if let Some(version) = &ifc_fqn.version {
                                {format!("@{version}")}
                            }
                        </h3>
                        <ul>
                            {submittable_fn_details}
                        </ul>
                    </section>
                }
             };

            let submittable_ifcs_fns = exports
                .iter()
                .filter(|(_, fn_details)| fn_details.iter().any(|f_d| f_d.submittable))
                .map(|(ifc_fqn, fn_details)| render_ifc_with_fns(ifc_fqn, fn_details))
                .collect::<Vec<_>>();

            html! { <>
                <h2>{&component.name}<span class="label">{component_type}</span></h2>
                {submittable_ifcs_fns}
            </>}
        });

    let wit = wit_state.deref().as_ref().map(|wit| {
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
        let output = WitPrinter::<OutputToHtml>::new()
            .print_all(&resolve, main_id, &ids)
            .expect("FIXME");

        Html::from_html_unchecked(output.output.into())
    });

    html! {<>
        <div class="container">
            <header>
                <h1>{"Components"}</h1>
            </header>

            <section class="component-selection">
                <ComponentTree components={components_with_idx} config={ComponentTreeConfig::ComponentsOnly {
                    selected_component_idx_state: selected_component_idx_state.clone()
                }
                } />
            </section>

            { component_detail }

            if let Some(wit) = wit {
                <h3>{"WIT"}</h3>
                <div class="code-block">
                    <pre>
                    { wit }
                    </pre>
                </div>
            }
        </div>

    </>}
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
