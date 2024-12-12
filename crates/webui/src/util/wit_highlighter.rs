use std::path::PathBuf;

use wit_component::{Output, WitPrinter};
use wit_parser::{Resolve, UnresolvedPackageGroup};

pub fn to_html(wit: &str) -> String {
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

    output.output.to_string()
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
