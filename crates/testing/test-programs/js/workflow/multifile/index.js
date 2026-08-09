import * as activity from 'testing:integration/activity';
import { computeTotal } from './lib/math.js';

export default function multifileWorkflow(a, b, c) {
    return computeTotal(activity.add(a, b), c);
}
