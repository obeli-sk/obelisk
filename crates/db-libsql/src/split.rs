//! Splits a SQL script into statements, a port of SQLite's `sqlite3_complete` state machine.

#[derive(Clone, Copy)]
enum Token {
    Semi,
    Ws,
    Other,
    Explain,
    Create,
    Temp,
    Trigger,
    End,
}

const START: u8 = 1;
// States: 0 invalid, 1 start, 2 normal, 3 explain, 4 create, 5 trigger, 6 semi, 7 end.
const TRANSITIONS: [[u8; 8]; 8] = [
    // Semi, Ws, Other, Explain, Create, Temp, Trigger, End
    [1, 0, 2, 3, 4, 2, 2, 2],
    [1, 1, 2, 3, 4, 2, 2, 2],
    [1, 2, 2, 2, 2, 2, 2, 2],
    [1, 3, 3, 2, 4, 2, 2, 2],
    [1, 4, 2, 2, 2, 4, 5, 2],
    [6, 5, 5, 5, 5, 5, 5, 5],
    [6, 6, 5, 5, 5, 5, 5, 7],
    [1, 7, 5, 5, 5, 5, 5, 5],
];

fn is_id_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

fn keyword(word: &[u8]) -> Token {
    let is = |keyword: &str| word.eq_ignore_ascii_case(keyword.as_bytes());
    if is("create") {
        Token::Create
    } else if is("trigger") {
        Token::Trigger
    } else if is("temp") || is("temporary") {
        Token::Temp
    } else if is("end") {
        Token::End
    } else if is("explain") {
        Token::Explain
    } else {
        Token::Other
    }
}

/// Returns the trimmed statements without their terminating semicolons, skipping empty ones.
pub(crate) fn split_statements(sql: &str) -> Result<Vec<&str>, String> {
    let bytes = sql.as_bytes();
    let find = |from: usize, needle: &[u8]| {
        bytes[from..]
            .windows(needle.len())
            .position(|window| window == needle)
            .map(|pos| from + pos)
    };
    let mut statements = Vec::new();
    let (mut state, mut start, mut i) = (0u8, 0, 0);
    while i < bytes.len() {
        let token = match bytes[i] {
            b';' => Token::Semi,
            b' ' | b'\r' | b'\t' | b'\n' | 0x0c => Token::Ws,
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let Some(end) = find(i + 2, b"*/") else {
                    return Err(format!("unterminated comment at byte {i}"));
                };
                i = end + 1;
                Token::Ws
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = find(i, b"\n").unwrap_or(bytes.len());
                Token::Ws
            }
            quote @ (b'[' | b'`' | b'"' | b'\'') => {
                let close = if quote == b'[' { b']' } else { quote };
                let Some(end) = find(i + 1, &[close]) else {
                    return Err(format!("unterminated `{}` at byte {i}", quote as char));
                };
                i = end;
                Token::Other
            }
            b if is_id_char(b) => {
                let len = bytes[i..].iter().take_while(|b| is_id_char(**b)).count();
                i += len - 1;
                keyword(&bytes[i + 1 - len..=i])
            }
            _ => Token::Other,
        };
        let previous = state;
        state = TRANSITIONS[usize::from(state)][token as usize];
        if matches!(token, Token::Semi) && state == START {
            if previous > START {
                statements.push(sql[start..i].trim());
            }
            start = i + 1;
        }
        i += 1;
    }
    if state > START {
        statements.push(sql[start..].trim());
    }
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::split_statements;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Case {
        name: String,
        sql: String,
        statements: Option<Vec<String>>,
        #[serde(default)]
        error: bool,
    }

    #[test]
    fn split_statements_should_match_the_shared_cases() {
        let cases: Vec<Case> =
            serde_json::from_str(include_str!("../split-statements.json")).unwrap();
        for case in cases {
            let actual = split_statements(&case.sql);
            if case.error {
                assert!(
                    actual.is_err(),
                    "{}: expected an error, got {actual:?}",
                    case.name
                );
            } else {
                assert_eq!(case.statements.unwrap(), actual.unwrap(), "{}", case.name);
            }
        }
    }
}
