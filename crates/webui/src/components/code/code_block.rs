use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct CodeBlockProps {
    pub source: Html,
}

#[function_component(CodeBlock)]
pub fn code_block(CodeBlockProps { source }: &CodeBlockProps) -> Html {
    html! {
        <div class="code-block">
            <pre>
                { source.clone() }
            </pre>
        </div>
    }
}
