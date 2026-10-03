// Test fixture: HMAC sign and verify using crypto.subtle.
// Returns the SHA-256 and SHA-512 signatures as hex, separated by ':'.
export default async function hmac_sign_verify(keyStr, message) {
    const enc = new TextEncoder();
    const signatures = [];
    for (const hash of ['SHA-256', 'SHA-512']) {
        const key = await crypto.subtle.importKey(
            'raw',
            enc.encode(keyStr),
            { name: 'HMAC', hash },
            false,
            ['sign', 'verify'],
        );
        const sig = await crypto.subtle.sign('HMAC', key, enc.encode(message));
        if (!await crypto.subtle.verify('HMAC', key, sig, enc.encode(message))) throw new Error(`${hash}: valid signature rejected`);
        const tampered = new Uint8Array(sig).slice();
        tampered[0] ^= 1;
        if (await crypto.subtle.verify({ name: 'HMAC' }, key, tampered, enc.encode(message))) throw new Error(`${hash}: tampered signature accepted`);
        signatures.push([...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join(''));
    }
    return signatures.join(':');
}
