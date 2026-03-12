// Call inline stub activity and provide its result via obelisk.stub().
export default function call_stub(id) {
    const js = obelisk.createJoinSet();
    const execId = js.submit('testing:integration/stubs.my-stub', [id]);
    console.log("stubbed id", execId);
    obelisk.stub(execId, { 'ok': 'stub-ok' });
    js.joinNext();
    return obelisk.getResult(execId).ok;
}
