// testing:js/activity.fetch-get: func(url: string, headers: list<tuple<string,string>>) -> result<string, string>
export default async function fetch_get(url /* string */, headersArr /* list<tuple<string,string>> */) {
    let headers = Object.fromEntries(headersArr);
    console.info("Fetching", url, headers);
    const resp = await fetch(url, {
        headers
    });
    try {
        const text = await resp.text();
        return text;
    } catch (e) {
        throw new Error(e); // cast to string
    }
}
