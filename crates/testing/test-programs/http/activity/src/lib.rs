use crate::exports::testing::http::http_get::Guest;
use obelisk::log::log::info;
use std::time::Duration;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn get(url: String) -> Result<String, String> {
        let resp = waki::Client::new()
            .get(&url)
            .connect_timeout(Duration::from_secs(1))
            .send()
            .map_err(|err| format!("{err:?}"))?;
        info(&format!("Got status code {}", resp.status_code()));
        let body = resp.body().map_err(|err| format!("{err:?}"))?;
        Ok(String::from_utf8_lossy(&body).into_owned())
    }

    fn get_successful(url: String) -> Result<String, String> {
        let resp = waki::Client::new()
            .get(&url)
            .connect_timeout(Duration::from_secs(1))
            .send()
            .map_err(|err| format!("{err:?}"))?;
        println!("Got status code {}", resp.status_code());
        if resp.status_code() >= 200 && resp.status_code() <= 299 {
            let body = resp.body().map_err(|err| format!("{err:?}"))?;
            Ok(String::from_utf8_lossy(&body).into_owned())
        } else {
            Err(format!("wrong status code: {}", resp.status_code()))
        }
    }
}
