
[WIT](https://component-model.bytecodealliance.org/design/wit.html) (WebAssembly Interface Types) is
the interface definition language of the WebAssembly Component Model. It describes the function
signatures that components export and import.

Declaring accurate types matters for two reasons:

- **Interoperability.** WIT is language-neutral — a JS workflow can call a Rust activity and vice
  versa without any glue code. The synthesized interface is the shared contract regardless of
  implementation language. This is the common pattern: download a pre-built Rust (or any WASM)
  activity and wire it from a JS workflow or webhook.
- **Runtime behaviour.** Obelisk uses the declared return type to interpret results and drive
  execution. For example, when an activity returns the `err` variant of a `result` type, Obelisk
  treats it as a failure and schedules a retry; an `ok` variant marks the execution as finished.

For JS components, Obelisk synthesizes WIT interfaces from the `ffqn`, `params`, and `return_type`
fields in `deployment.toml` — no WIT files needed. Rust components define their own WIT files and
use the `bindgen!` macro to generate bindings. See
JS components and
Rust components for details.

WIT identifiers must use **kebab-case** (e.g. `sleep-millis`, not `sleep_millis`).

## WIT syntax

### Package declaration

A WIT file begins with a `package` declaration giving the namespace and package name, optionally
with a version:

```wit
package tutorial:activity;
// or with version:
package tutorial:activity@1.0.0;
```

The package name forms the first part of an interface's
FFQN.

### Interface and function declarations

An `interface` block groups related functions. Each function is declared with a name, parameter
names and types, and a return type:

```wit
package tutorial:activity;

interface activity-sleepy {
    step: func(idx: u64, sleep-millis: u64) -> result<u64>;
    health-check: func() -> result;
}
```

### World declaration

A `world` declares what a component exports (provides) and imports (depends on):

```wit
package any:any;

world any {
    export tutorial:activity/activity-sleepy;   // functions this component provides
    import obelisk:log/log@1.0.0;               // functions this component calls
}
```

Obelisk only inspects exported interfaces to identify the functions a component provides. The
world's own package name and world name are ignored.

### Named type declarations

Types like `record`, `variant`, `enum`, and `flags` can be named and reused across functions:

```wit
interface account {
    record user-info {
        login: string,
        id: u64,
    }

    enum status {
        active,
        suspended,
    }

    variant fetch-error {
        not-found,
        rate-limited(u32),   // payload: seconds until reset
    }

    get-user: func(login: string) -> result<user-info, fetch-error>;
    get-status: func(login: string) -> result<status>;
}
```

In `deployment.toml` for JS components, named types can also be written **inline** — Obelisk assigns
generated names (`t0`, `t1`, …) automatically:

```toml
params = [
  { name = "point", type = "record { x: u32, y: u32 }" },
]
return_type = "result<variant { found(string), not-found }>"
```

## Primitive types

| WIT type | Description             |
| -------- | ----------------------- |
| `bool`   | Boolean                 |
| `u8`     | Unsigned 8-bit integer  |
| `u16`    | Unsigned 16-bit integer |
| `u32`    | Unsigned 32-bit integer |
| `u64`    | Unsigned 64-bit integer |
| `s8`     | Signed 8-bit integer    |
| `s16`    | Signed 16-bit integer   |
| `s32`    | Signed 32-bit integer   |
| `s64`    | Signed 64-bit integer   |
| `f32`    | 32-bit float            |
| `f64`    | 64-bit float            |
| `char`   | Unicode scalar value    |
| `string` | UTF-8 string            |

## Compound types

| WIT syntax                      | Description                                 |
| ------------------------------- | ------------------------------------------- |
| `option<T>`                     | Optional value                              |
| `result`                        | Success/failure with no payloads            |
| `result<T>`                     | Success with value, failure with no payload |
| `result<_, E>`                  | Success with no payload, failure with error |
| `result<T, E>`                  | Success with value, failure with error      |
| `list<T>`                       | Variable-length sequence                    |
| `tuple<T1, T2, ...>`            | Fixed-length heterogeneous sequence         |
| `record { field-name: T, ... }` | Object with named fields                    |
| `variant { case-name(T), ... }` | Tagged union                                |
| `enum { case-name, ... }`       | Enumeration (one of a fixed set)            |
| `flags { flag-name, ... }`      | Bit-flag set                                |

## JSON encoding

Arguments to `obelisk.call` and results are JSON-encoded. The mapping between WIT types and JSON:

| WIT type                            | JSON encoding                                                         |
| ----------------------------------- | --------------------------------------------------------------------- |
| `bool`                              | `true` / `false`                                                      |
| integers, floats                    | JSON number                                                           |
| `char`, `string`                    | JSON string                                                           |
| `option<T>` — some                  | JSON value of T                                                       |
| `option<T>` — none                  | `null`                                                                |
| `result<T, E>` — ok                 | `{"ok": <T value>}`                                                   |
| `result<T, E>` — err                | `{"err": <E value>}`                                                  |
| `result` / `result<T>` — no payload | `{"ok": null}` / `{"err": null}`                                      |
| `list<T>`                           | JSON array                                                            |
| `tuple<T1, T2, ...>`                | JSON array `[val1, val2, ...]`                                        |
| `record { field-name: T, ... }`     | JSON object; kebab-case keys become snake_case: `{"field_name": val}` |
| `variant { case-name(T) }`          | No payload: `"case_name"`. With payload: `{"case_name": val}`         |
| `enum { case-name }`                | `"case_name"` (kebab-case → snake_case)                               |
| `flags { flag-name }`               | Array of active flag strings: `["flag_name"]`                         |

Note: kebab-case WIT identifiers become snake_case in JSON keys and values.
