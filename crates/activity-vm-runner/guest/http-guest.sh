#!/bin/sh
set -eu

allowed=$1
shift

mark_phase() {
  marker="/obelisk-activity-vm-http/phase-$1"
  if [ -n "${2:-}" ]; then
    printf '%s\n' "$2" > "$marker.tmp"
    mv "$marker.tmp" "$marker"
  else
    : > "$marker"
  fi
}

mark_phase guest-launcher
if [ ! -d /nix/store ]; then
  mark_phase store-mount-failed "/nix/store is not a directory"
  exit 125
fi
mark_phase store-mounted "$(grep ' /nix/store' /proc/mounts || true)"

if command -v ip >/dev/null 2>&1; then
  ip link set lo up
elif command -v ifconfig >/dev/null 2>&1; then
  ifconfig lo up
else
  mark_phase network-failed "neither ip nor ifconfig is available to enable loopback"
  exit 125
fi
printf '%s\n' 'nameserver 127.0.0.1' > /etc/resolv.conf
proxy_log=/obelisk-activity-vm-http/proxy.log
/usr/local/libexec/obelisk/obelisk-activity-vm-http-proxy \
  /obelisk-activity-vm-http "$allowed" 2>"$proxy_log" &
proxy_pid=$!
while [ ! -f /tmp/obelisk-activity-vm-network-ready ] || \
      [ ! -f /tmp/obelisk-activity-vm-ca.pem ]; do
  if ! kill -0 "$proxy_pid" 2>/dev/null; then
    wait "$proxy_pid" || status=$?
    mark_phase network-failed "proxy exited with status ${status:-0}: $(cat "$proxy_log")"
    exit 125
  fi
  sleep 1
done
mark_phase network-ready

export SSL_CERT_FILE=/tmp/obelisk-activity-vm-ca.pem

mode=$1
shift
case "$mode" in
  --entrypoint) command="$1"; shift ;;
  --script)
    script="$1"
    shift
    IFS= read -r shebang < "$script"
    interpreter=${shebang#\#!}
    if [ "$interpreter" = "$shebang" ] || [ -z "$interpreter" ]; then
      echo "activity VM inline script must start with a shebang" >&2
      exit 126
    fi
    set -- $interpreter "$script" "$@"
    command="$1"
    shift
    ;;
  *) echo "unknown activity VM invocation mode: $mode" >&2; exit 126 ;;
esac

resolved_command=$(command -v "$command" 2>&1 || true)
mark_phase command-start "${resolved_command:-$command}"
stdout=/obelisk-activity-vm-http/stdout
stderr=/obelisk-activity-vm-http/stderr
: > "$stdout"
: > "$stderr"
set +e
if [ -n "${OBELISK_ACTIVITY_VM_STDIN:-}" ]; then
  "$command" "$@" < "$OBELISK_ACTIVITY_VM_STDIN" > "$stdout" 2> "$stderr"
else
  "$command" "$@" > "$stdout" 2> "$stderr"
fi
status=$?
set -e
printf '%s\n' "$status" > /obelisk-activity-vm-http/exit-code.tmp
mv /obelisk-activity-vm-http/exit-code.tmp /obelisk-activity-vm-http/exit-code
# The host consumes exit-code. Keep the VM launcher successful so init performs the
# same clean shutdown path for successful and unsuccessful activity commands.
exit 0
