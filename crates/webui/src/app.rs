use crate::{
    components::{
        component_list_page::ComponentListPage,
        debugger::debugger_view::DebuggerView,
        execution_detail_page::ExecutionLogPage,
        execution_list_page::{ExecutionFilter, ExecutionListPage},
        execution_stub_submit_page::ExecutionStubResultPage,
        execution_submit_page::ExecutionSubmitPage,
        not_found::NotFound,
        trace::trace_view::TraceView,
    },
    grpc::{
        ffqn::FunctionFqn,
        grpc_client::{self, ComponentId, ExecutionId},
        ifc_fqn::IfcFqn,
        version::VersionType,
    },
};
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use std::{fmt::Display, ops::Deref, rc::Rc, str::FromStr};
use yew::prelude::*;
use yew_router::prelude::*;

#[derive(Clone, PartialEq)]
pub struct AppState {
    pub components_by_id: HashMap<ComponentId, Rc<grpc_client::Component>>,
    pub comopnents_by_exported_ifc: HashMap<IfcFqn, Rc<grpc_client::Component>>,
    pub ffqns_to_details:
        hashbrown::HashMap<FunctionFqn, (grpc_client::FunctionDetail, grpc_client::ComponentId)>,
}

#[derive(Clone, PartialEq, derive_more::Display)]
pub enum ExecutionsCursor {
    #[display("{_0}")]
    ExecutionId(ExecutionId),
    #[display("C_{_0:?}")]
    CreatedAt(DateTime<Utc>),
}

impl FromStr for ExecutionsCursor {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once("_") {
            Some(("E", rest)) => Ok(ExecutionsCursor::ExecutionId(ExecutionId {
                id: format!("E_{rest}"),
            })),
            Some(("C", date)) => DateTime::from_str(date)
                .map(ExecutionsCursor::CreatedAt)
                .map_err(|_| ()),
            _ => Err(()),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct BacktraceVersions(Vec<VersionType>);
const BACKTRACE_VERSIONS_SEPARATOR: char = '_';
impl Display for BacktraceVersions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, version) in self.0.iter().enumerate() {
            if idx == 0 {
                write!(f, "{version}")?;
            } else {
                write!(f, "{BACKTRACE_VERSIONS_SEPARATOR}{version}")?;
            }
        }
        Ok(())
    }
}

impl FromStr for BacktraceVersions {
    type Err = ();

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut versions = Vec::new();
        for split in input.split(BACKTRACE_VERSIONS_SEPARATOR) {
            let version: VersionType = split.parse().map_err(|_| ())?;
            versions.push(version);
        }
        Ok(BacktraceVersions(versions))
    }
}
impl From<VersionType> for BacktraceVersions {
    fn from(value: VersionType) -> Self {
        BacktraceVersions(vec![value])
    }
}
impl BacktraceVersions {
    pub fn last(&self) -> VersionType {
        *self.0.last().expect("must contain at least one element")
    }
    pub fn step_into(&self) -> BacktraceVersions {
        let mut ret = self.clone();
        ret.0.push(0);
        ret
    }
    pub fn change(&self, version: VersionType) -> BacktraceVersions {
        let mut ret = self.clone();
        *ret.0.last_mut().expect("must contain at least one element") = version;
        ret
    }
    pub fn step_out(&self) -> Option<BacktraceVersions> {
        let mut ret = self.clone();
        ret.0.pop();
        if ret.0.is_empty() { None } else { Some(ret) }
    }
}
impl Default for BacktraceVersions {
    fn default() -> Self {
        BacktraceVersions(vec![0])
    }
}

#[derive(Clone, Routable, PartialEq)]
pub enum Route {
    #[at("/")]
    Home,
    #[at("/components")]
    ComponentList,
    #[at("/component/:component_id")]
    Component {
        component_id: grpc_client::ComponentId,
    },
    #[at("/execution/submit/:ffqn")]
    ExecutionSubmit { ffqn: FunctionFqn },
    #[at("/execution/stub/:ffqn/:execution_id")]
    ExecutionStubResult {
        ffqn: FunctionFqn,
        execution_id: ExecutionId,
    },
    #[at("/execution/list")]
    ExecutionList,
    #[at("/execution/list/older/:cursor")]
    ExecutionListOlder { cursor: ExecutionsCursor },
    #[at("/execution/list/older_inc/:cursor")]
    ExecutionListOlderIncluding { cursor: ExecutionsCursor },
    #[at("/execution/list/newer/:cursor")]
    ExecutionListNewer { cursor: ExecutionsCursor },
    #[at("/execution/list/newer_inc/:cursor")]
    ExecutionListNewerIncluding { cursor: ExecutionsCursor },
    #[at("/execution/list/ffqn/:ffqn")]
    ExecutionListByFfqn { ffqn: FunctionFqn },
    #[at("/execution/:execution_id/log")]
    ExecutionLog {
        execution_id: grpc_client::ExecutionId,
    },
    #[at("/execution/:execution_id")]
    ExecutionTrace {
        execution_id: grpc_client::ExecutionId,
    },
    #[at("/execution/:execution_id/debug")]
    ExecutionDebugger {
        execution_id: grpc_client::ExecutionId,
    },
    #[at("/execution/:execution_id/debug/:versions")]
    ExecutionDebuggerWithVersions {
        execution_id: grpc_client::ExecutionId,
        versions: BacktraceVersions,
    },
    #[not_found]
    #[at("/404")]
    NotFound,
}

impl Route {
    pub fn render(route: Route) -> Html {
        match route {
            Route::Home | Route::ExecutionList => html! { <ExecutionListPage /> },
            Route::ComponentList => html! { <ComponentListPage /> },
            Route::Component { component_id } => {
                html! { <ComponentListPage maybe_component_id={Some(component_id)}/> }
            }
            Route::ExecutionSubmit { ffqn } => html! { <ExecutionSubmitPage {ffqn} /> },
            Route::ExecutionStubResult { ffqn, execution_id } => {
                html! { <ExecutionStubResultPage {ffqn}  {execution_id} /> }
            }
            Route::ExecutionLog { execution_id } => {
                html! { <ExecutionLogPage {execution_id} /> }
            }
            Route::ExecutionListOlder { cursor } => {
                html! { <ExecutionListPage filter={ExecutionFilter::Older { cursor, including_cursor: false }} /> }
            }
            Route::ExecutionListOlderIncluding { cursor } => {
                html! { <ExecutionListPage filter={ExecutionFilter::Older { cursor, including_cursor: true }} /> }
            }
            Route::ExecutionListNewer { cursor } => {
                html! { <ExecutionListPage filter={ExecutionFilter::Newer { cursor, including_cursor: false }} /> }
            }
            Route::ExecutionListNewerIncluding { cursor } => {
                html! { <ExecutionListPage filter={ExecutionFilter::Newer { cursor, including_cursor: true }} /> }
            }

            Route::ExecutionListByFfqn { ffqn } => {
                html! { <ExecutionListPage filter={ExecutionFilter::Ffqn { ffqn } } /> }
            }
            Route::ExecutionTrace { execution_id } => {
                html! { <TraceView {execution_id} /> }
            }
            Route::ExecutionDebugger { execution_id } => {
                html! { <DebuggerView {execution_id} versions={BacktraceVersions::from(0)} /> }
            }
            Route::ExecutionDebuggerWithVersions {
                execution_id,
                versions,
            } => {
                html! { <DebuggerView {execution_id} versions={versions} /> }
            }
            Route::NotFound => html! { <NotFound /> },
        }
    }
}

#[derive(PartialEq, Properties)]
pub struct AppProps {
    pub components_by_id: HashMap<ComponentId, Rc<grpc_client::Component>>,
    pub comopnents_by_exported_ifc: HashMap<IfcFqn, Rc<grpc_client::Component>>,
}

#[function_component(App)]
pub fn app(
    AppProps {
        components_by_id,
        comopnents_by_exported_ifc,
    }: &AppProps,
) -> Html {
    let mut ffqns_to_details = hashbrown::HashMap::new();
    for (component_id, component) in components_by_id {
        for exported_fn_detail in component.exports.iter() {
            let ffqn =
                FunctionFqn::from_fn_detail(exported_fn_detail).expect("ffqn should be parseable");
            ffqns_to_details.insert(ffqn, (exported_fn_detail.clone(), component_id.clone()));
        }
    }
    let app_state = use_state(|| AppState {
        components_by_id: components_by_id.clone(),
        ffqns_to_details,
        comopnents_by_exported_ifc: comopnents_by_exported_ifc.clone(),
    });
    html! {
        <ContextProvider<AppState> context={app_state.deref().clone()}>
            <div class="container">
                <BrowserRouter>
                    <nav>
                        <Link<Route> to={Route::ExecutionList }>
                            {"Execution List"}
                        </Link<Route>>
                        {" "}
                        <Link<Route> to={Route::ComponentList }>
                            {"Component list"}
                        </Link<Route>>

                    </nav>
                    <Switch<Route> render={Route::render} />
                </BrowserRouter>
            </div>
        </ContextProvider<AppState>>
    }
}
