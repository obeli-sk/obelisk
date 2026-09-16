# Exact QEMU Node migration smoke test

This proves the canonical QEMU engine, c2w migration image, runtime 9p pack,
and guest command independently of the Wasmtime host implementation.

Prepare the matching Emscripten runtime directory from the canonical build:

```sh
cp out.js out.mjs
cp out.js qemu-system-x86_64
npm install
```

The directory must contain `out.mjs`, `qemu-system-x86_64`, and
`qemu-system-x86_64.worker.js`. Then run:

```sh
QEMU_IMAGE_DIR=/path/to/image \
QEMU_WASM=/path/to/qemu-system-x86_64.wasm \
QEMU_GLUE_DIR=/path/to/canonical-glue \
node --experimental-wasm-stack-switching node-exact9b-minimal.mjs
```

The stopped migration image is resumed through the QEMU monitor. The harness
then sends the c2w `=\n` handshake and succeeds only after observing
`QEMU_NODE_EXACT9B_ECHO_OK` from `/bin/echo`.
