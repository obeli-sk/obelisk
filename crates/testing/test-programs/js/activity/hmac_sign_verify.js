// Test fixture: HMAC-SHA256 sign using crypto.subtle.
// Accepts a key string and a message string, signs the message, and returns
// the signature as a hex string for the caller to verify.
export default async function hmac_sign_verify(keyStr, message) {
    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey(
        'raw',
        enc.encode(keyStr),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign'],
    );
    const sig = await crypto.subtle.sign('HMAC', key, enc.encode(message));
    return [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join('');
}
