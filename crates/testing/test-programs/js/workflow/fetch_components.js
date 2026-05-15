import { fetchGet } from 'testing:js/activity';

export default function fetch_components() {
    const result = fetchGet("http://localhost:5005/v1/components", [["accept", "application/json"]]);
    console.log('child result:', result);
    return result;
}
