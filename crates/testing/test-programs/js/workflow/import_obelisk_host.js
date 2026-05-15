// Use obelisk: namespace imports instead of the obelisk.* global API
import { joinSetCreate, joinSetClose, joinNext } from 'obelisk:workflow/workflow-support@5.1.0';
import { addSubmit } from 'testing:integration-obelisk-ext/activity';
import { info } from 'obelisk:log/log@1.0.0';

export default function add_via_activity(a, b) {
    const js = joinSetCreate();
    info('Created join set, submitting add(' + a + ', ' + b + ')');
    addSubmit(js, a, b);
    const response = joinNext(js);
    info('Got response: ' + JSON.stringify(response));
    joinSetClose(js);
    return response.ok ? 'ok' : 'err';
}
