export default function testMathRandom(params) {
    const r1 = Math.random();
    const r2 = Math.random();
    const r3 = Math.random();
    return JSON.stringify({
        r1, r2, r3,
        inRange: r1 >= 0 && r1 < 1 && r2 >= 0 && r2 < 1 && r3 >= 0 && r3 < 1
    });
}
