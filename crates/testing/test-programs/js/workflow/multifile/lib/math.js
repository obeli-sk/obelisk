import * as activity from 'testing:integration/activity';

export function computeTotal(partial, c) {
    return activity.add(partial, c);
}
