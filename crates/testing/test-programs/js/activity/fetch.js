async function fetch_get(params) {
    console.info("Fetching " + params[0]);
    const resp = await fetch(params[0]);
    const text = await resp.text();
    return text;
}
