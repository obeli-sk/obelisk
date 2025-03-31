use crate::{
    app::Route,
    components::{code::syntect_code_block::SyntectCodeBlock, execution_status::ExecutionStatus},
    grpc::{
        execution_id::ExecutionIdExt as _,
        grpc_client::{
            self,
            execution_event::{self, history_event},
            get_backtrace_request, ComponentId, ExecutionEvent, ExecutionId, GetBacktraceResponse,
            GetBacktraceSourceRequest, JoinSetResponseEvent,
        },
        version::VersionType,
    },
    pages::execution_detail_page::{compute_join_next_to_response, event_to_detail},
};
use gloo::timers::future::TimeoutFuture;
use hashbrown::HashMap;
use log::{debug, trace};
use std::{ops::Deref as _, path::PathBuf};
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct DebuggerViewProps {
    pub execution_id: grpc_client::ExecutionId,
    pub version: Option<VersionType>,
}

#[function_component(DebuggerView)]
pub fn debugger_view(
    DebuggerViewProps {
        execution_id,
        version,
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
        HashMap<(ExecutionId, Option<VersionType>), GetBacktraceResponse>,
    > = use_state(Default::default);
    let sources_state: UseStateHandle<HashMap<(ComponentId, String), Option<String>>> =
        use_state(Default::default);

    use_effect_with((execution_id.clone(), *version), {
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
                        filter: Some(match version {
                            Some(version) => get_backtrace_request::Filter::Specific(
                                get_backtrace_request::Specific { version },
                            ),
                            None => get_backtrace_request::Filter::First(
                                get_backtrace_request::First {},
                            ),
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
                        sources.insert(key, None);
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
            for ((component_id, file), _) in sources.iter().filter(|(_key, val)| val.is_none()) {
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
                    let backtrace_src_response = match backtrace_src_response {
                        Err(status) if status.code() == tonic::Code::NotFound => None,
                        Ok(ok) => Some(ok.into_inner().content),
                        err @ Err(_) => panic!("{err:?}"),
                    };
                    debug!("Got backtrace_src_response {backtrace_src_response:?}");
                    let mut sources = sources_state.deref().clone();
                    sources.insert((component_id, file), backtrace_src_response);
                    sources_state.set(sources);
                });
            }
        }
    });

    let (execution_log, is_finished) = {
        let events = events_and_responses_state.events_state.deref();
        let responses = &events_and_responses_state.responses_state.0;
        let join_next_version_to_response = compute_join_next_to_response(events, responses);
        let execution_log = events
            .iter()
            .filter(|event| {
                let event_inner = event.event.as_ref().expect("event is sent by the server");
                matches!(
                    event_inner,
                    execution_event::Event::Created(_) | execution_event::Event::Finished(_)
                ) || (event.backtrace_id.is_some()
                    && !matches!(
                        event_inner,
                        execution_event::Event::HistoryVariant(execution_event::HistoryEvent {
                            event: Some(history_event::Event::JoinSetCreated(_))
                        })
                    ))
            })
            .map(|event| event_to_detail(execution_id, event, &join_next_version_to_response))
            .collect::<Vec<_>>();
        let is_finished = matches!(
            events.last(),
            Some(ExecutionEvent {
                event: Some(execution_event::Event::Finished(_)),
                ..
            })
        );
        (execution_log, is_finished)
    };

    let backtrace = if let Some(backtrace_response) = backtraces_state
        .deref()
        .get(&(execution_id.clone(), *version))
    {
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
                        if let Some(source) = sources_state
                            .deref()
                            .get(&(component_id.clone(), file.clone()))
                            .and_then(|s| s.clone())
                        {
                            let language = PathBuf::from(file)
                                .extension()
                                .map(|e| e.to_string_lossy().to_string());
                            htmls.push(html! {
                                <SyntectCodeBlock {source} {language} focus_line={Some(line as usize)}/>
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
        <div class="execution-header">
            <h3>{{execution_id.render_execution_parts(false, |execution_id| Route::ExecutionDebugger { execution_id })}}</h3>
            <ExecutionStatus execution_id={execution_id.clone()} status={None} print_finished_status={true} />
        </div>
        <div class="trace-layout-container">
            <div class="trace-view">
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
