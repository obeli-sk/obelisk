use once_cell::sync::Lazy;
use std::cmp::min;
use std::rc::Rc;
use syntect::html::{ClassStyle, ClassedHTMLGenerator};
use syntect::parsing::SyntaxSet;
use syntect::util::LinesWithEndings;
use yew::{prelude::*, virtual_dom::VNode};

const DEFAULT_CONTEXT_LINES: usize = 3;
const DEFAULT_THEME: &str = "base16-ocean.dark"; // NB: Sync with build.rs

pub static SYNTAX_SET: Lazy<SyntaxSet> = Lazy::new(SyntaxSet::load_defaults_newlines);

pub fn highlight_code_line_by_line(source: &str, language_ext: Option<&str>) -> Vec<(Html, usize)> {
    const DEFAULT_LANGUAGE: &str = "txt";
    let language_ext = language_ext.unwrap_or(DEFAULT_LANGUAGE);
    let start = web_sys::window().unwrap().performance().unwrap().now();
    let syntax = SYNTAX_SET
        .find_syntax_by_extension(language_ext)
        .unwrap_or_else(|| SYNTAX_SET.find_syntax_plain_text());

    let mut output_lines = Vec::new();
    for (line_num, line) in LinesWithEndings::from(source).enumerate() {
        let line_num = line_num + 1; // Store with 1-based line number
        let mut highlighter =
            ClassedHTMLGenerator::new_with_class_style(syntax, &SYNTAX_SET, ClassStyle::Spaced);
        if let Err(_err) = highlighter.parse_html_for_line_which_includes_newline(line) {
            // Display as plain text
            let line = line.to_html();
            output_lines.push((line, line_num));
        } else {
            // Note: The generated HTML usually doesn't include the outer <span> or <pre> tags per line.
            // We wrap each line in a span or div later in the component.
            let line = highlighter.finalize();
            let line = VNode::from_html_unchecked(line.into());
            output_lines.push((line, line_num));
        }
    }
    let end = web_sys::window().unwrap().performance().unwrap().now();
    log::trace!("Highlighted in {}ms", end - start);
    output_lines
}

#[derive(Properties, PartialEq)]
pub struct CodeBlockProps {
    pub source: Rc<[(Html, usize)]>,
    pub focus_line: Option<usize>,
}

enum ExpandDirection {
    Above,
    Below,
    All,
}

#[function_component(SyntectCodeBlock)]
pub fn code_block(props: &CodeBlockProps) -> Html {
    let total_lines = props.source.len();

    // State for the visible range [start_line_idx, end_line_idx) (0-based index)
    let visible_start_idx = use_state(|| {
        if let Some(focus_line) = props.focus_line {
            // Calculate initial start index based on focus_line and context
            // focus_line is 1-based, convert to 0-based index
            let focus_idx = focus_line.saturating_sub(1);
            focus_idx.saturating_sub(DEFAULT_CONTEXT_LINES)
        } else {
            0 // Show all from the beginning if no focus line
        }
    });

    let visible_end_idx = use_state(|| {
        if let Some(focus_line) = props.focus_line {
            // Calculate initial end index based on focus_line and context
            let focus_idx = focus_line.saturating_sub(1);
            min(total_lines, focus_idx + DEFAULT_CONTEXT_LINES + 1)
        } else {
            total_lines // Show all to the end if no focus line
        }
    });

    // Clamp values on prop change if needed (e.g., source shrinks)
    use_effect_with((total_lines, props.focus_line), {
        let visible_start_idx = visible_start_idx.clone();
        let visible_end_idx = visible_end_idx.clone();
        move |(total_lines, focus_line)| {
            let (new_start, new_end) = if let Some(focus) = focus_line {
                let focus_idx = focus.saturating_sub(1);
                (
                    focus_idx.saturating_sub(DEFAULT_CONTEXT_LINES),
                    min(*total_lines, focus_idx + DEFAULT_CONTEXT_LINES + 1),
                )
            } else {
                (0, *total_lines)
            };
            if *visible_start_idx != new_start {
                visible_start_idx.set(new_start);
            }
            if *visible_end_idx != new_end {
                visible_end_idx.set(new_end);
            }
        }
    });

    let handle_expand = {
        let visible_start_idx = visible_start_idx.clone();
        let visible_end_idx = visible_end_idx.clone();
        Callback::from(move |direction: ExpandDirection| {
            match direction {
                ExpandDirection::Above => {
                    let current_start = *visible_start_idx;
                    // Show more lines above, e.g., double the context or a fixed step
                    let new_start = current_start.saturating_sub(DEFAULT_CONTEXT_LINES * 2);
                    visible_start_idx.set(new_start);
                }
                ExpandDirection::Below => {
                    let current_end = *visible_end_idx;
                    // Show more lines below
                    let new_end = min(total_lines, current_end + DEFAULT_CONTEXT_LINES * 2);
                    visible_end_idx.set(new_end);
                }
                ExpandDirection::All => {
                    visible_start_idx.set(0);
                    visible_end_idx.set(total_lines);
                }
            }
        })
    };

    let show_expand_above = *visible_start_idx > 0;
    let show_expand_below = *visible_end_idx < total_lines;

    // Slice the highlighted lines based on the visible range state
    let lines_to_render = props
        .source
        .get(*visible_start_idx..*visible_end_idx)
        .unwrap_or_default();

    html! {
        <div class={classes!("code-block-container", format!("theme-{DEFAULT_THEME}"))}>
            { if show_expand_above {
                html!{
                    <button class="expand-button expand-above" onclick={handle_expand.reform(move |_| ExpandDirection::Above)}>
                        { format!("Expand {} lines above", min(*visible_start_idx, DEFAULT_CONTEXT_LINES*2)) }
                    </button>
                }
            } else {
                html!{}
            } }

            { if show_expand_above || show_expand_below {
                 html!{
                    <button class="expand-button expand-all" onclick={handle_expand.reform(move |_| ExpandDirection::All)}>
                        { "Expand All" }
                    </button>
                 }
            } else {
                html!{}
            } }

            <div class="code-block">
                <div class="line-numbers">
                    { for lines_to_render.iter().map(|(_, line_num)| html! {
                        <span class="line-number">{ line_num }</span>
                    }) }
                </div>
                <pre class="code-content"><code>
                    { for lines_to_render.iter().map(|(line_html, line_num)| {
                        let is_focused = props.focus_line == Some(*line_num);
                        let line_class = classes!(is_focused.then_some("line-focused"));
                        // Wrap each line in a div or span to apply line-specific classes like focus highlight
                        html! { <span class={line_class}>{ line_html.clone() }</span> }
                    }) }
                </code></pre>
            </div>

            { if show_expand_below {
                html!{
                    <button class="expand-button expand-below" onclick={handle_expand.reform(move |_| ExpandDirection::Below)}>
                         { format!("Expand {} lines below", min(total_lines - *visible_end_idx, DEFAULT_CONTEXT_LINES*2)) }
                    </button>
                }
            } else {
                 html!{}
            } }
        </div>
    }
}
