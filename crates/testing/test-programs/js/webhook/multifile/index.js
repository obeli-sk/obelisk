import { renderJson } from './lib/render.js';

export default function multifileWebhook(_request) {
    return renderJson({ ok: true, message: 'multifile webhook works' });
}
