async function fetch_get(url /* string */, headersArr /* list<tuple<string,string>> */) {
    let headers = Object.fromEntries(headersArr);
    console.info("Fetching", url, headers);
    const resp = await fetch(url, {
        headers
    });
    const text = await resp.text();
    return text;
}
