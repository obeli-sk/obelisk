export default function cpu_activity() {
    for (let i = 0; ; i++) {
        if (i % 10000000 == 0) {
            console.log(i);
        }
    }
}
