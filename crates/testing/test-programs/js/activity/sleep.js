export default async function sleep(milliseconds) {
    await new Promise(r => setTimeout(r, milliseconds));
}
