export default function read_env(key) {
    const value = obelisk.env(key);
    if (value === undefined) {
        throw "env var not found: " + key;
    }
    return value;
}
