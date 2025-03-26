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
    let total_duration = (props.root_last_event_at - props.root_scheduled_at)
        .to_std()
        .expect("root_scheduled_at must be lower or equal to root_last_event");

    let intervals: Vec<_> = props
        .data
        .busy_intervals(props.root_scheduled_at, props.root_last_event_at)
        .into_iter()
        .map(|(start_percentage, busy_percentage)| {
            html! {
                <div
                class="busy-duration-line"
                style={format!("width: {}%; margin-left: {}%", busy_percentage, start_percentage)}
            >
            </div>
            }
        })
        .collect();

    let has_children = !props.data.children().is_empty();
    let mut children_html = Html::default();

    if has_children {
        children_html = html! {
            <div class="indented-children"> // Wrap children in a container
                { for props.data.children().iter().map(|child| html! {
                    <ExecutionStep
                        data={TraceData::Child(child.clone())}
                        root_scheduled_at={props.root_scheduled_at}
                        root_last_event_at={props.root_last_event_at}
                         />
                })}
            </div>
        };
    }
    let tooltip = if let TraceData::Root(_) = props.data {
        format!(
            "Total: {total_duration:?}, busy: {:?}",
            props.data.busy_duration(props.root_last_event_at)
        )
    } else {
        format!("{:?}", props.data.busy_duration(props.root_last_event_at))
    };
    let name = props.data.name().to_string();
    html! {
        <div class="execution-step">
            <div class="step-row">
                <span class="step-icon">{"▶"}</span>
                <span class="step-name" title={name.clone()}>{name}</span>
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
