use crate::exports::testing::http::http_get::Guest;
use exports::testing::http::http_get;
use std::time::Duration;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn get(url: String) -> Result<String, String> {
        let resp = Self::get_resp(url)?;
        Ok(String::from_utf8_lossy(&resp.body).into_owned())
    }

    fn get_successful(url: String) -> Result<String, String> {
        let resp = Self::get_resp(url)?;
        if resp.status_code >= 200 && resp.status_code <= 299 {
            Ok(String::from_utf8_lossy(&resp.body).into_owned())
        } else {
            assert!(resp.status_code != 418, "418 causes trap");
            Err(format!("wrong status code: {}", resp.status_code))
        }
    }

    fn get_resp(url: String) -> Result<http_get::Response, String> {
        let resp = waki::Client::new()
            .get(&url)
            .connect_timeout(Duration::from_secs(1))
            .send()
            .map_err(|err| format!("{err:?}"))?;
        let status_code = resp.status_code();
        let body = resp.body().map_err(|err| format!("{err:?}"))?;
        Ok(http_get::Response { body, status_code })
    }

    fn get_stargazers() -> Result<http_get::Stargazers, String> {
        Ok(http_get::Stargazers {
            cursor: "cursor".to_string(),
            logins: "logins".to_string(),
        })
    }
}
