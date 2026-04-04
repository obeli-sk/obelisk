export default function testDateNow() {
    const now = Date.now();
    return JSON.stringify({
        now,
        isNumber: typeof now === 'number'
    });
}
