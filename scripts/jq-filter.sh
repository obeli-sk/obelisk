jq -s '
  def to_milliseconds:
    if . | test("µs$") then
      (. | sub("µs$"; "") | tonumber * 0.001)
    elif . | test("ms$") then
      (. | sub("ms$"; "") | tonumber)
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
    })
  })
'
