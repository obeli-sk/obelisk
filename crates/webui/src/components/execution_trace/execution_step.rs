use super::data::TraceData;
use std::time::Duration;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ExecutionStepProps {
    pub data: TraceData,
    pub total_duration: Duration,
}

#[function_component(ExecutionStep)]
pub fn execution_step(props: &ExecutionStepProps) -> Html {
    let total_percentage =
        (props.total_duration.as_millis() as f64 / props.total_duration.as_millis() as f64) * 100.0;
    let start_percentage = (props.data.started_at().as_millis() as f64
        / props.total_duration.as_millis() as f64)
        * 100.0;

    let busy_percentage = {
        if let Some(finished_at) = props.data.finished_at() {
            let busy_duration = finished_at - props.data.started_at();
            (busy_duration.as_millis() as f64 / props.total_duration.as_millis() as f64) * 100.0
        } else {
            // Fill until the end
            100.0 - start_percentage + 1.0
        }
    };

    let has_children = !props.data.children().is_empty();
    let mut children_html = Html::default();

    if has_children {
        children_html = html! {
            <div class="indented-children"> // Wrap children in a container
                { for props.data.children().iter().map(|child| html! {
                    <ExecutionStep
                        data={TraceData::Child(child.clone())}
                        total_duration={props.total_duration} />
                })}
            </div>
        };
    }
    let tooltip = if let TraceData::Root(_) = props.data {
        format!("Total duration: {:?}", props.total_duration)
    } else if let Some(finished_at) = props.data.finished_at() {
        let duration = finished_at - props.data.started_at();
        format!("{duration:?}")
    } else {
        "No response (yet)".to_string()
    };
    let name = props.data.name().to_string();
    html! {
        <div class="execution-step">
            <div class="step-row">
                <span class="step-icon">{"▶"}</span>
                <span class="step-name" title={name.clone()}>{name}</span>
                <div class="relative-duration-container">
                    <div class="total-duration-line" style={format!("width: {}%", total_percentage)} title={tooltip}>
                        <div
                            class="busy-duration-line"
                            style={format!("width: {}%; margin-left: {}%", busy_percentage, start_percentage)}
                        >
                        </div>
                    </div>
                </div>
            </div>
            {children_html}
        </div>
    }
}
