export default function busy() {
    sleep();
    return "ok";
}

function sleep_via_activity() {
    for (let i = 0; i < 30; i++) {
        dynamic.call('testing:integration/sleep-activity.sleep', [300]);
    }
}

function sleep() {
    for (let i = 0; i < 30; i++) {
        obelisk.sleep({ milliseconds: 400 });
    }
}
import * as obelisk from "obelisk:workflow@1.0.0";
import * as dynamic from "obelisk:workflow-dynamic@1.0.0";
