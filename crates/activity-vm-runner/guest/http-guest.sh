#!/bin/sh
set -eu

shift

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

set +e
if [ -n "${OBELISK_ACTIVITY_VM_STDIN:-}" ]; then
  "$command" "$@" < "$OBELISK_ACTIVITY_VM_STDIN"
else
  "$command" "$@"
fi
status=$?
set -e
printf '\nOBELISK_ACTIVITY_VM_EXIT_CODE=%s\n' "$status"
exit 0
