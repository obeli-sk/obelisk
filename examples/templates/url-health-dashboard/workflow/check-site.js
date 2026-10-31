import { checkUrl } from "starter:app/activity";
import * as obelisk from "obelisk:workflow@1.0.0";

// Once the activity's retries are exhausted, record the outage in the result.
export default function checkSite(url) {
  try {
    return { url, check: { ok: checkUrl(url) } };
  } catch (e) {
    if (!(e instanceof obelisk.ChildError)) throw e;
    return { url, check: { err: String(e.value ?? e.message) } };
  }
}
