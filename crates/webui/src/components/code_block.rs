use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct CodeBlockProps {
    pub source: String,
}

#[function_component(CodeBlock)]
pub fn code_block(CodeBlockProps { source }: &CodeBlockProps) -> Html {
    html! {
        <div class="code-block">
            <pre>
                { Html::from_html_unchecked(AttrValue::from(source.clone())) }
            </pre>
        </div>
    }
}
