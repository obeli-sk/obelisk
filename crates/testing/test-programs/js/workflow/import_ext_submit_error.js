import { readEnvSubmit } from 'testing:integration-obelisk-ext/activity-env';
import * as obelisk from 'obelisk:workflow@1.0.0';

export default function import_ext_submit_error() {
    const js = obelisk.createJoinSet();
    try {
        readEnvSubmit(js, 'x'.repeat(300));
    } catch (error) {
        if (error instanceof Error && error.message.includes('ValueTooLarge')) {
            return 'typed submit threw';
        }
        throw error;
    }
    throw new Error('typed submit returned without throwing');
}
