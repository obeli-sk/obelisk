// Use ES module import to schedule an activity (instead of obelisk.schedule)
import { addSchedule } from 'testing:integration-obelisk-schedule/activity';

export default function schedule_activity(a, b) {
    const execId = addSchedule(null, a, b);
    console.log('Scheduled execution:', execId);
    return execId;
}
