import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { openpty } from 'xterm-pty';

const imageDir = process.env.QEMU_IMAGE_DIR;
const wasmPath = process.env.QEMU_WASM;
const glueDir = process.env.QEMU_GLUE_DIR;
if (!imageDir || !wasmPath || !glueDir) {
  throw new Error('QEMU_IMAGE_DIR, QEMU_WASM, and QEMU_GLUE_DIR are required');
}
const runtimeDir = path.resolve(glueDir);
const { default: createQemu } = await import(pathToFileURL(path.join(runtimeDir, 'out.mjs')));

const started = performance.now();
const { master, slave } = openpty();
let terminal = '';
let continued = false;
master.onWrite(([data, acknowledge]) => {
  process.stdout.write(data);
  terminal += new TextDecoder().decode(data);
  acknowledge();
  if (terminal.includes('QEMU_NODE_EXACT9B_ECHO_OK')) {
    console.error(`ECHO_MS=${Math.round(performance.now() - started)}`);
    process.exit(0);
  }
  if (!continued && terminal.includes('(qemu)')) {
    continued = true;
    master.ldisc.writeFromLower([...Buffer.from('cont\n'), 1, 99, 61, 10]);
    console.error(`CONT_AND_INIT_SENT_MS=${Math.round(performance.now() - started)}`);
  }
});
setTimeout(() => {
  master.ldisc.writeFromLower([1, 99]);
  console.error(`MONITOR_SWITCH_SENT_MS=${Math.round(performance.now() - started)}`);
}, 1000);
await createQemu({
  pty: slave,
  mainScriptUrlOrBlob: path.join(runtimeDir, 'qemu-system-x86_64'),
  arguments: [
    '-incoming', 'file:/image/vm.state', '-nographic', '-m', '128M',
    '-cpu', 'qemu64,+rdrand', '-device', 'virtio-rng-pci',
    '-accel', 'tcg,tb-size=500,thread=multi', '-smp', '1,sockets=1',
    '-L', '/image/', '-drive', 'if=virtio,format=raw,file=/image/rootfs.bin',
    '-kernel', '/image/bzImage', '-nic', 'none',
    '-append', 'earlyprintk=ttyS0,115200n8 console=ttyS0,115200n8 slub_debug=F root=/dev/vda rootwait acpi=off ro virtio_net.napi_tx=false loglevel=7 QEMU_MODE=1 init=/sbin/tini -- /sbin/init',
    '-virtfs', 'local,path=/,mount_tag=wasi0,security_model=passthrough,id=wasi0',
    '-virtfs', 'local,path=/pack,mount_tag=wasi1,security_model=passthrough,id=wasi1',
  ],
  locateFile: (file) => file.endsWith('.wasm') ? wasmPath : path.join(runtimeDir, file),
  preRun: [(mod) => {
    mod.FS.mkdir('/image');
    mod.FS.mkdir('/pack');
    for (const name of fs.readdirSync(imageDir)) {
      mod.FS.writeFile(`/image/${name}`, fs.readFileSync(`${imageDir}/${name}`));
    }
    mod.FS.writeFile('/pack/info', 'c: /bin/echo QEMU_NODE_EXACT9B_ECHO_OK\n');
  }],
  print: (line) => console.log(line),
  printErr: (line) => console.error(line),
  onExit: (status) => console.error(`QEMU_EXIT=${status} ELAPSED_MS=${Math.round(performance.now() - started)}`),
});
