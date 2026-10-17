import { checkUrlSubmit, checkUrlAwaitNext } from "starter:app-obelisk-ext/activity";
import * as obelisk from "obelisk:workflow@1.0.0";

// Submit every check first so they run in parallel, then collect them in input order.
export default function checkBatch(urls) {
  const pending = urls.map((url) => {
    const joinSet = obelisk.createJoinSet();
    checkUrlSubmit(joinSet, url);
    return { url, joinSet };
  });
  return pending.map(({ url, joinSet }) => {
    try {
      return { url, check: { ok: checkUrlAwaitNext(joinSet) } };
    } catch (e) {
      if (!(e instanceof obelisk.ChildError)) throw e;
      return { url, check: { err: e.value ?? e.message } };
    }
  });
}
