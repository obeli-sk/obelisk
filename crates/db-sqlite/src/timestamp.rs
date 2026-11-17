use jiff::Timestamp;
use rusqlite::{ToSql, types::ToSqlOutput};
use std::fmt::Write as _;

pub(crate) struct SqlTimestamp(pub Timestamp);

impl SqlTimestamp {
    pub(crate) fn text(&self) -> String {
        // backcompat: 0.43 stores UTC timestamps with a space, grouped fractional digits and +00:00.
        let mut text = self.0.strftime("%Y-%m-%d %H:%M:%S").to_string();
        let nanos = self.0.as_nanosecond().rem_euclid(1_000_000_000);
        if nanos != 0 {
            if nanos % 1_000_000 == 0 {
                write!(text, ".{:03}", nanos / 1_000_000).unwrap();
            } else if nanos % 1_000 == 0 {
                write!(text, ".{:06}", nanos / 1_000).unwrap();
            } else {
                write!(text, ".{nanos:09}").unwrap();
            }
        }
        text.push_str("+00:00");
        text
    }
}

impl ToSql for SqlTimestamp {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.text()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_compare_and_roundtrip_with_existing_rows() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE timestamps (at TEXT NOT NULL)")
            .unwrap();
        for legacy in [
            "1969-12-31 23:59:59.500+00:00",
            "2026-01-01 00:00:00+00:00",
            "2026-01-01 00:00:00.100+00:00",
            "2026-01-01 00:00:00.123400+00:00",
            "2026-01-01 00:00:00.123456789+00:00",
        ] {
            let at: Timestamp = legacy.parse().unwrap();
            db.execute("INSERT INTO timestamps VALUES (?1)", [legacy])
                .unwrap();
            let matched: Timestamp = db
                .query_row(
                    "SELECT at FROM timestamps WHERE at = ?1",
                    [SqlTimestamp(at)],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(at, matched);
            db.execute("INSERT INTO timestamps VALUES (?1)", [SqlTimestamp(at)])
                .unwrap();
            let count: u32 = db
                .query_row(
                    "SELECT count(*) FROM timestamps WHERE at = ?1",
                    [legacy],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 2);
        }
    }
}
