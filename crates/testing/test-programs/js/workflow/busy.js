export default function busy() {
    sleep();
    return "ok";
}

function sleep_via_activity() {
    for (let i = 0; i < 30; i++) {
        obelisk.call('testing:integration/sleep-activity.sleep', [300]);
    }
}

function sleep() {
    for (let i = 0; i < 30; i++) {
        obelisk.sleep({ milliseconds: 300 });
    }
}


function cpu_activity() {
    for (let i = 0; ; i++) {
        if (i % 100000 == 0) {
            console.log(i);
        }
    }
}
