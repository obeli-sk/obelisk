// Webhook that schedules an activity using ES module import
import { addSchedule } from 'testing:integration-obelisk-schedule/activity';

export default function handle(request) {
    const execId = addSchedule(null, 3, 4);
    return Response.json({ execId });
}
