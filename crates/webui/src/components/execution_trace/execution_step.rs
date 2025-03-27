use super::data::TraceData;
use chrono::{DateTime, Utc};
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionStepProps {
    pub data: TraceData,
    pub root_scheduled_at: DateTime<Utc>,
    pub root_last_event_at: DateTime<Utc>,
}

#[function_component(ExecutionStep)]
pub fn execution_step(props: &ExecutionStepProps) -> Html {
    let intervals: Vec<_> = props
        .data
        .busy()
        .iter()
        .map(|interval| {
            let (start_percentage, busy_percentage) =
                interval.as_percentage(props.root_scheduled_at, props.root_last_event_at);
            html! {
                <div
                class={classes!("busy-duration-line", interval.status.get_css_class())}
                title={interval.title.clone()}
                style={format!("width: {}%; margin-left: {}%", busy_percentage, start_percentage)}
            >
            </div>
            }
        })
        .collect();

    let children_html = if !props.data.children().is_empty() {
        html! {
            <div class="indented-children"> // Wrap children in a container
                { for props.data.children().iter().map(|child| html! {
                    <ExecutionStep
                        data={child.clone()}
                        root_scheduled_at={props.root_scheduled_at}
                        root_last_event_at={props.root_last_event_at}
                         />
                })}
            </div>
        }
    } else {
        Html::default()
    };
    let tooltip = if let TraceData::Root(root) = &props.data {
        format!(
            "Total: {:?}, busy: {:?}",
            root.total_duration(),
            props.data.busy_duration(props.root_last_event_at)
        )
    } else {
        format!("{:?}", props.data.busy_duration(props.root_last_event_at))
    };
    let last_status = props.data.busy().last().map(|interval| interval.status);

    html! {
        <div class="execution-step">
            <div class="step-row">
                <span class="step-icon">{"▶"}</span>
                <span class="step-name" title={props.data.title().to_string()}>{props.data.name().clone()}</span>
                if let Some(status) = last_status {
                    <span class="step-status">{status.to_string()}</span>
                }
                <div class="relative-duration-container">
                    <div class="total-duration-line" style="width: 100%" title={tooltip}>
                        {intervals}
                    </div>
                </div>
            </div>
            {children_html}
        </div>
    }
}
