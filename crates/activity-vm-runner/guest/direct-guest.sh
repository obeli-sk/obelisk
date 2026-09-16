#!/bin/sh
set -eu

mark_phase() {
  printf '%s\n' "${2:-ready}" > "/obelisk-activity-vm-http/phase-$1"
}

mark_phase guest-launcher
if [ "${OBELISK_ACTIVITY_VM_NIX_STORE:-0}" = 1 ] && [ ! -d /nix/store ]; then
  mark_phase store-mount-failed "/nix/store is not a directory"
  exit 125
fi
mark_phase store-mounted

shift
mode=$1
shift
case "$mode" in
  --entrypoint) command=$1; shift ;;
  --script)
    script=$1
    shift
    IFS= read -r shebang < "$script"
    interpreter=${shebang#\#!}
    if [ "$interpreter" = "$shebang" ] || [ -z "$interpreter" ]; then
      echo "activity VM inline script must start with a shebang" >&2
      exit 126
    fi
    set -- $interpreter "$script" "$@"
    command=$1
    shift
    ;;
  *) echo "unknown activity VM invocation mode: $mode" >&2; exit 126 ;;
esac

mark_phase command-start "$(command -v "$command" 2>&1 || printf '%s' "$command")"
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
printf '%s\n' "$status" > /obelisk-activity-vm-http/exit-code
exit 0
