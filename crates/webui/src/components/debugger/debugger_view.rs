use crate::{
    app::{BacktraceVersions, Route},
    components::{
        code::syntect_code_block::{SyntectCodeBlock, highlight_code_line_by_line},
        execution_detail::utils::{compute_join_next_to_response, event_to_detail},
        execution_header::{ExecutionHeader, ExecutionLink},
    },
    grpc::{
        execution_id::ExecutionIdExt as _,
        grpc_client::{
            self, ComponentId, ExecutionEvent, ExecutionId, GetBacktraceResponse,
            GetBacktraceSourceRequest, JoinSetResponseEvent,
            execution_event::{self, history_event},
            get_backtrace_request, join_set_response_event,
        },
        version::VersionType,
    },
};
use gloo::timers::future::TimeoutFuture;
use hashbrown::HashMap;
use log::{debug, trace};
use std::{collections::BTreeSet, ops::Deref as _, path::PathBuf, rc::Rc};
use yew::prelude::*;
use yew_router::prelude::Link;

#[derive(Properties, PartialEq)]
pub struct DebuggerViewProps {
    pub execution_id: grpc_client::ExecutionId,
    pub versions: BacktraceVersions,
}

#[derive(Clone, PartialEq)]
enum SourceCodeState {
    Requested,
    Found(Rc<[(Html, usize)]>),
    NotFoundOrErr,
}

#[function_component(DebuggerView)]
pub fn debugger_view(
    DebuggerViewProps {
        execution_id,
        versions,
    }: &DebuggerViewProps,
) -> Html {
    let events_state: UseStateHandle<Vec<ExecutionEvent>> = use_state(Vec::new);
    let responses_state: UseStateHandle<(
        HashMap<grpc_client::JoinSetId, Vec<JoinSetResponseEvent>>,
        u32, /* Cursor */
    )> = use_state(|| (HashMap::new(), 0));
    let is_fetching_state = use_state(|| (execution_id.clone(), 0)); // Retrigger if not on the last page

    let events_and_responses_state = EventsAndResponsesState {
        events_state,
        responses_state,
        is_fetching_state,
    };
    use_effect_with(
        (
            execution_id.clone(),
            events_and_responses_state.is_fetching_state.deref().clone(),
        ),
        {
            let events_and_responses_state = events_and_responses_state.clone();
            move |(execution_id, is_fetching)| {
                events_and_responses_state.hook(execution_id, is_fetching, true)
            }
        },
    );

    let backtraces_state: UseStateHandle<
        HashMap<(ExecutionId, VersionType), GetBacktraceResponse>,
    > = use_state(Default::default);
    let sources_state: UseStateHandle<HashMap<(ComponentId, String), SourceCodeState>> =
        use_state(Default::default);
    let version = versions.last();
    use_effect_with((execution_id.clone(), version), {
        let backtraces_state = backtraces_state.clone();
        let sources_state = sources_state.clone(); // Write a request to obtain the sources.
        move |(execution_id, version)| {
            let execution_id = execution_id.clone();
            let version = *version;
            if backtraces_state.contains_key(&(execution_id.clone(), version)) {
                trace!("Prevented GetBacktrace fetch");
                return;
            }
            wasm_bindgen_futures::spawn_local(async move {
                trace!("GetBacktraceRequest {execution_id} {version:?}");
                let base_url = "/api";
                let mut execution_client =
                    grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let backtrace_response = execution_client
                    .get_backtrace(tonic::Request::new(grpc_client::GetBacktraceRequest {
                        execution_id: Some(execution_id.clone()),
                        filter: Some(if version > 0 {
                            get_backtrace_request::Filter::Specific(
                                get_backtrace_request::Specific { version },
                            )
                        } else {
                            get_backtrace_request::Filter::First(get_backtrace_request::First {})
                        }),
                    }))
                    .await;
                let backtrace_response = match backtrace_response {
                    Err(status) if status.code() == tonic::Code::NotFound => return,
                    Ok(ok) => ok.into_inner(),
                    err @ Err(_) => panic!("{err:?}"),
                };
                debug!("Got backtrace_response {backtrace_response:?}");
                let mut sources = sources_state.deref().clone();
                let component_id = backtrace_response
                    .component_id
                    .clone()
                    .expect("GetBacktraceResponse.component_id is sent");
                for file in backtrace_response
                    .wasm_backtrace
                    .as_ref()
                    .expect("GetBacktraceResponse.wasm_backtrace is sent")
                    .frames
                    .iter()
                    .flat_map(|frame| frame.symbols.iter())
                    .filter_map(|frame_symbol| frame_symbol.file.as_ref())
                {
                    let key = (component_id.clone(), file.clone());
                    if !sources.contains_key(&key) {
                        sources.insert(key, SourceCodeState::Requested);
                    }
                }
                sources_state.set(sources);

                let mut backtraces: HashMap<_, _> = backtraces_state.deref().clone();
                backtraces.insert((execution_id, version), backtrace_response);
                backtraces_state.set(backtraces);
            });
        }
    });

    use_effect_with(sources_state.deref().clone(), {
        let sources_state = sources_state.clone();
        move |sources| {
            for ((component_id, file), _) in sources
                .iter()
                .filter(|(_key, val)| **val == SourceCodeState::Requested)
            {
                let component_id = component_id.clone();
                let file = file.clone();
                let sources_state = sources_state.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    trace!("GetBacktraceSourceRequest {component_id} {file}");
                    let base_url = "/api";
                    let mut execution_client =
                        grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                            tonic_web_wasm_client::Client::new(base_url.to_string()),
                        );
                    let backtrace_src_response = execution_client
                        .get_backtrace_source(tonic::Request::new(GetBacktraceSourceRequest {
                            component_id: Some(component_id.clone()),
                            file: file.clone(),
                        }))
                        .await;
                    let source_code_state = match backtrace_src_response {
                        Err(err) => {
                            log::warn!("Cannot obtain source `{file}` - {err:?}");
                            SourceCodeState::NotFoundOrErr
                        }
                        Ok(ok) => {
                            let language = PathBuf::from(&file)
                                .extension()
                                .map(|e| e.to_string_lossy().to_string());
                            SourceCodeState::Found(Rc::from(highlight_code_line_by_line(
                                &ok.into_inner().content,
                                language.as_deref(),
                            )))
                        }
                    };
                    let mut sources = sources_state.deref().clone();
                    sources.insert((component_id, file), source_code_state);
                    sources_state.set(sources);
                });
            }
        }
    });

    let events = events_and_responses_state.events_state.deref();
    let responses = &events_and_responses_state.responses_state.0;
    let join_next_version_to_response = compute_join_next_to_response(events, responses);
    let backtrace_response = backtraces_state
        .deref()
        .get(&(execution_id.clone(), version));
    let execution_log = events
        .iter()
        .filter(|event| {
            let event_inner = event.event.as_ref().expect("event is sent by the server");
            matches!(
                event_inner,
                execution_event::Event::Created(_) | execution_event::Event::Finished(_)
            ) || event.backtrace_id.is_some()
        })
        .map(|event| {
            event_to_detail(
                execution_id,
                event,
                &join_next_version_to_response,
                ExecutionLink::Debug,
                // is_selected
                backtrace_response
                    .and_then(|br| br.wasm_backtrace.as_ref())
                    .map(|b| {
                        b.version_min_including <= event.version
                            && b.version_max_excluding > event.version
                    })
                    .unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();

    let is_finished = matches!(
        events.last(),
        Some(ExecutionEvent {
            event: Some(execution_event::Event::Finished(_)),
            ..
        })
    );

    // Step Out
    let step_out = if let Some(parent_id) = execution_id.parent_id() {
        let versions = versions.step_out().unwrap_or_default();
        html! {
            <Link<Route> to={Route::ExecutionDebuggerWithVersions { execution_id: parent_id, versions }}>
            {"Step Out"}
            </Link<Route>>
        }
    } else {
        html! {
            <div class="disabled">
                {"Step Out"}
            </div>
        }
    };
    let backtrace = if let Some(backtrace_response) = backtrace_response {
        let mut htmls = Vec::new();
        let mut seen_positions = hashbrown::HashSet::new();
        let wasm_backtrace = backtrace_response
            .wasm_backtrace
            .as_ref()
            .expect("`wasm_backtrace` is sent");
        let component_id = backtrace_response
            .component_id
            .as_ref()
            .expect("`GetBacktraceResponse.component_id` is sent");

        // Add Step Prev, Next, Into, Out
        let backtrace_versions: BTreeSet<VersionType> = events
            .iter()
            .filter_map(|event| event.backtrace_id)
            .collect();
        htmls.push(if let Some(backtrace_prev) = backtrace_versions
            .range(..wasm_backtrace.version_min_including)
            .next_back()
            .copied()
        {
            let versions = versions.change(backtrace_prev);
            html! {
                <Link<Route> to={Route::ExecutionDebuggerWithVersions { execution_id: execution_id.clone(), versions } }>
                    {"Step Prev"}
                </Link<Route>>
            }
        } else {
            html! {
                <div class="disabled">
                 {"Step Prev"}
                </div>
            }
        });
        htmls.push(if let Some(backtrace_next) = backtrace_versions
            .range(wasm_backtrace.version_max_excluding..)
            .next()
            .copied()
        {
            let versions = versions.change(backtrace_next);
            html! {
                <Link<Route> to={Route::ExecutionDebuggerWithVersions { execution_id: execution_id.clone(), versions } }>
                    {"Step Next"}
                </Link<Route>>
            }
        } else {
            html! {
                <div class="disabled">
                    {"Step Next"}
                </div>
            }
        });

        // Step Into
        let version_child_request =
            if wasm_backtrace.version_max_excluding - wasm_backtrace.version_min_including == 3 {
                // only happens on one-off join sets where 3 events share the same backtrace.
                wasm_backtrace.version_min_including + 1
            } else {
                wasm_backtrace.version_min_including
            };
        htmls.push(match events.get(usize::try_from(version_child_request).expect("u32 must be convertible to usize")) {
                Some(ExecutionEvent {
                    event:
                        Some(execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
                            event: Some(
                                history_event::Event::JoinSetRequest(history_event::JoinSetRequest{join_set_request: Some(history_event::join_set_request::JoinSetRequest::ChildExecutionRequest(
                                    history_event::join_set_request::ChildExecutionRequest{child_execution_id: Some(child_execution_id)}
                                )
                            ), ..
                            })),
                        })),
                    ..
                }) => {
                    let versions = versions.step_into();
                    html!{
                        <Link<Route> to={Route::ExecutionDebuggerWithVersions { execution_id: child_execution_id.clone(), versions } }>
                            {"Step Into"}
                        </Link<Route>>
                    }
                },

                Some(event@ExecutionEvent {
                    event:
                        Some(execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
                            event: Some(
                                history_event::Event::JoinNext(..)),
                        })),
                    ..
                }) => {
                    if let Some(JoinSetResponseEvent { response: Some(join_set_response_event::Response::ChildExecutionFinished(join_set_response_event::ChildExecutionFinished{
                        child_execution_id: Some(child_execution_id), ..
                    })), .. }) = join_next_version_to_response.get(&event.version) {
                        let versions = versions.step_into();
                        html!{
                            <Link<Route> to={Route::ExecutionDebuggerWithVersions { execution_id: child_execution_id.clone(), versions } }>
                               {"Step Into"}
                            </Link<Route>>
                        }
                    } else {
                        Html::default()
                    }
                }

                _ => Html::default()
            }
        );

        htmls.push(html!{
            <p>
                {"Backtrace version: "}
                if wasm_backtrace.version_min_including == wasm_backtrace.version_max_excluding - 1 {
                    {wasm_backtrace.version_min_including}
                } else {
                    {wasm_backtrace.version_min_including}{"-"}{wasm_backtrace.version_max_excluding - 1}
                }
            </p>
        });

        for (i, frame) in wasm_backtrace.frames.iter().enumerate() {
            htmls.push(html! {
                <p>
                    {format!("{i}: {}, function: {}", frame.module, frame.func_name)}
                </p>
            });

            for symbol in &frame.symbols {
                // Print location.
                let location = match (&symbol.file, symbol.line, symbol.col) {
                    (Some(file), Some(line), Some(col)) => format!("{file}:{line}:{col}"),
                    (Some(file), Some(line), None) => format!("{file}:{line}"),
                    (Some(file), None, None) => file.clone(),
                    _ => "unknown location".to_string(),
                };
                let mut line = format!("    at {location}");

                // Print function name if it's different from frameinfo
                match &symbol.func_name {
                    Some(func_name) if *func_name != frame.func_name => {
                        line.push_str(&format!(" - {func_name}"));
                    }
                    _ => {}
                }
                htmls.push(html! {
                    <p>
                        {line}
                    </p>
                });

                // Print source file.
                if let (Some(file), Some(line)) = (&symbol.file, symbol.line) {
                    let new_position = seen_positions.insert((file.clone(), line));
                    if new_position {
                        if let Some(SourceCodeState::Found(source)) = sources_state
                            .deref()
                            .get(&(component_id.clone(), file.clone()))
                        {
                            htmls.push(html! {
                                <SyntectCodeBlock source={source.clone()} focus_line={Some(line as usize)}/>
                            });
                        }
                    }
                }
            }
        }
        htmls.to_html()
    } else if is_finished {
        "No backtrace found for this execution".to_html()
    } else {
        "Loading...".to_html()
    };

    html! {<>
        <ExecutionHeader execution_id={execution_id.clone()} link={ExecutionLink::Debug} />

        <div class="trace-layout-container">
            <div class="trace-view">
                {step_out}
                {backtrace}
            </div>
            <div class="trace-detail">
                {execution_log}
            </div>
        </div>

    </>}
}

#[derive(Clone)]
pub struct EventsAndResponsesState {
    pub events_state: UseStateHandle<Vec<ExecutionEvent>>,
    pub responses_state: UseStateHandle<(
        HashMap<grpc_client::JoinSetId, Vec<JoinSetResponseEvent>>,
        u32, /* Cursor */
    )>,
    pub is_fetching_state: UseStateHandle<(ExecutionId, usize)>,
}
impl EventsAndResponsesState {
    pub fn hook(
        &self,
        execution_id: &ExecutionId,
        is_fetching: &(ExecutionId, usize),
        include_backtrace_id: bool,
    ) {
        const PAGE: u32 = 100;
        const TIMEOUT_MILLIS: u32 = 1000;
        if execution_id != &is_fetching.0 {
            debug!("Cleaning up the state after execution id change");
            self.events_state.set(Default::default());
            self.responses_state.set(Default::default());
            self.is_fetching_state.set((execution_id.clone(), 0)); // retrigger
            return;
        }
        trace!("Setting is_fetching_state=true {execution_id}");
        {
            let execution_id = execution_id.clone();
            let events_state = self.events_state.clone();
            let responses_state = self.responses_state.clone();
            let is_fetching_state = self.is_fetching_state.clone();
            let is_fetching = is_fetching.1;
            wasm_bindgen_futures::spawn_local(async move {
                let mut events = events_state.deref().clone();
                let version_from = events.last().map(|e| e.version + 1).unwrap_or_default();
                let (mut responses, responses_cursor_from) = responses_state.deref().clone();
                trace!("list_execution_events_and_responses {execution_id} {version_from}");
                let base_url = "/api";
                let mut execution_client =
                    grpc_client::execution_repository_client::ExecutionRepositoryClient::new(
                        tonic_web_wasm_client::Client::new(base_url.to_string()),
                    );
                let new_events_and_responses = execution_client
                    .list_execution_events_and_responses(
                        grpc_client::ListExecutionEventsAndResponsesRequest {
                            execution_id: Some(execution_id.clone()),
                            version_from,
                            events_length: PAGE,
                            responses_cursor_from,
                            responses_length: PAGE,
                            responses_including_cursor: responses_cursor_from == 0,
                            include_backtrace_id,
                        },
                    )
                    .await
                    .unwrap()
                    .into_inner();
                debug!(
                    "Got {} events, {} responses",
                    new_events_and_responses.events.len(),
                    new_events_and_responses.responses.len()
                );
                events.extend(new_events_and_responses.events);
                let last_event = events.last().expect("not found is sent as an error");
                let is_finished =
                    matches!(last_event.event, Some(execution_event::Event::Finished(_)));
                debug!("Setting events of {execution_id} to {:?}", events.first());
                events_state.set(events);
                {
                    let responses_cursor_from = new_events_and_responses
                        .responses
                        .last()
                        .map(|r| r.cursor)
                        .unwrap_or(responses_cursor_from);
                    for response in new_events_and_responses.responses {
                        let response = response
                            .event
                            .expect("`event` is sent in `ResponseWithCursor`");
                        let join_set_id = response
                            .join_set_id
                            .clone()
                            .expect("`join_set_id` is sent in `JoinSetResponseEvent`");
                        let execution_responses = responses.entry(join_set_id).or_default();
                        execution_responses.push(response);
                    }
                    responses_state.set((responses, responses_cursor_from));
                }
                if is_finished {
                    debug!("Execution Finished {execution_id} ");
                } else {
                    trace!("Timeout: start");
                    TimeoutFuture::new(TIMEOUT_MILLIS).await;
                    trace!("Timeout: Triggering refetch");
                    is_fetching_state.set((execution_id, is_fetching + 1)); // Retrigger
                }
            });
        }
    }
}
