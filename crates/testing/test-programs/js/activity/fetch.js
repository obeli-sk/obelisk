async function fetch_get(url) {
    console.info("Fetching " + url);
    const resp = await fetch(url);
    const text = await resp.text();
    return text;
}
