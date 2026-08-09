import { greet } from './lib/greeter.js';
import { exclaim } from './lib/util.js';

export default function multifileActivity(name) {
    return exclaim(greet(name));
}
