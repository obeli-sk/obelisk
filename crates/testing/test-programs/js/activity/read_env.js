export default function read_env(key) {
    const value = process.env[key];
    if (value === undefined) {
        throw "env var not found: " + key;
    }
    return value;
}
