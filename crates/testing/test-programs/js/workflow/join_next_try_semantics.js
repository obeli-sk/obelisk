// Exercise JS joinNextTry() semantics: pending -> undefined, success -> value,
// failure -> throw ChildExecutionError, exhausted -> host error class.
import { addSubmit } from 'testing:integration-obelisk-ext/activity';
import { myStubSubmit } from 'testing:integration-obelisk-ext/stubs';
import { myStubStub } from 'testing:integration-obelisk-stub/stubs';

export default function join_next_try_semantics(id) {
    const pendingJs = obelisk.createJoinSet();
    addSubmit(pendingJs, 1, 2);
    const pendingResult = pendingJs.joinNextTry();
    // Deterministic because join-next-try host function never fetches new responses.
    if (pendingResult !== undefined) {
        throw `joinNextTry should return undefined while the join set is still pending, got: ${JSON.stringify(pendingResult)} (${typeof pendingResult})`;
    }
    pendingJs.close();

    const delayJs = obelisk.createJoinSet();
    delayJs.submitDelay({ milliseconds: 1 });
    obelisk.sleep({ milliseconds: 10 });
    const delayResult = delayJs.joinNextTry();
    if (delayResult !== null) {
        throw `expected completed delay to produce null, got: ${JSON.stringify(delayResult)}`;
    }

    const okJs = obelisk.createJoinSet();
    const okExecId = myStubSubmit(okJs, id);
    myStubStub(okExecId, { ok: 'stub-ok' }); // Writes the response to current execution
    obelisk.sleep({ milliseconds: 1 }); // Submits a delay request.
    // Stub response must have been fetched as it was written before the delay response.
    const okResult = okJs.joinNextTry();
    if (okResult !== 'stub-ok') {
        throw `unexpected ok result: ${JSON.stringify(okResult)}`;
    }
    if (okJs.lastId !== okExecId) {
        throw `lastId mismatch: ${okJs.lastId} !== ${okExecId}`;
    }

    try {
        okJs.joinNextTry(); // Simulate programming error
        throw 'expected join set exhaustion error';
    } catch (e) {
        if (!(e instanceof obelisk.JoinSetExhaustedError)) {
            throw `expected JoinSetExhaustedError, got: ${e}`;
        }
        if (e.code !== 'OBELISK_JOIN_SET_EXHAUSTED') {
            throw `unexpected exhaustion error code: ${e.code}`;
        }
    }

    const errJs = obelisk.createJoinSet();
    const errExecId = myStubSubmit(errJs, id + 1);
    myStubStub(errExecId, { err: 'stub-err' });
    obelisk.sleep({ milliseconds: 1 });
    try {
        errJs.joinNextTry();
        throw 'expected stub error';
    } catch (e) {
        if (!(e instanceof obelisk.ChildExecutionError)) {
            throw `expected ChildExecutionError, got: ${e}`;
        }
        if (e.value !== 'stub-err') {
            throw `unexpected child err value: ${e.value}`;
        }
        if (e.childId !== errExecId) {
            throw `unexpected childId: ${e.childId} !== ${errExecId}`;
        }
    }

    const blockingErrJs = obelisk.createJoinSet();
    const blockingErrExecId = myStubSubmit(blockingErrJs, id + 2);
    myStubStub(blockingErrExecId, { err: 'stub-err-blocking' });
    obelisk.sleep({ milliseconds: 1 });
    try {
        blockingErrJs.joinNext();
        throw 'expected blocking stub error';
    } catch (e) {
        if (!(e instanceof obelisk.ChildExecutionError)) {
            throw `expected ChildExecutionError, got: ${e}`;
        }
        if (e.value !== 'stub-err-blocking') {
            throw `unexpected blocking child err value: ${e.value}`;
        }
    }

    return okResult;
}
