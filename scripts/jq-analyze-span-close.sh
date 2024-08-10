#!/usr/bin/env bash

# This script expects a log file in JSON format to be on stdin,
# extracts and groups span close events and returns their duration aggregates.
# It can be used for finding spans that get slower on every iteration,
# which can be observed by a big difference between min and max duration.

set -exuo pipefail

jq -s '
  def to_milliseconds:
    if . | test("µs$") then
      (. | sub("µs$"; "") | tonumber * 0.001)
    elif . | test("ms$") then
      (. | sub("ms$"; "") | tonumber)
    elif . | test("s$") then
      (. | sub("s$"; "") | tonumber * 1000)
    else
      null
    end;

  map(select(.fields.message == "close")) |
  group_by(.target) | map({
    target: .[0].target,
    rest: group_by(.span.name) | map({
      span_name: .[0].span.name,
      count: length,
      min_busy_time: map(.fields["time.busy"] | to_milliseconds) | min,
      max_busy_time: map(.fields["time.busy"] | to_milliseconds) | max,
      min_idle_time: map(.fields["time.idle"] | to_milliseconds) | min,
      max_idle_time: map(.fields["time.idle"] | to_milliseconds) | max,

    })
  })
'
