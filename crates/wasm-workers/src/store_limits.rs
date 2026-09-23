//! Per-slot memory enforcement for the `(workload, runtime)` cells of `[limits]`.

use wasmtime::ResourceLimiter;

/// Bounds a store's linear memories in total, unlike `StoreLimitsBuilder::memory_size`, which
/// caps each one separately. Exceeding it errors rather than returning `false`, so the guest traps
/// instead of seeing a catchable -1 from `memory.grow`. See `concurrency-limits.md`.
#[derive(Debug, Default)]
pub struct StoreMemoryLimiter {
    /// `None` leaves the store unbounded.
    max_total: Option<u64>,
    total: u64,
}

impl StoreMemoryLimiter {
    #[must_use]
    pub fn new(max_total: Option<u64>) -> Self {
        Self {
            max_total,
            total: 0,
        }
    }
}

impl ResourceLimiter for StoreMemoryLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let Some(max_total) = self.max_total else {
            return Ok(true);
        };
        let grown = u64::try_from(desired.saturating_sub(current)).unwrap_or(u64::MAX);
        let requested = self.total.saturating_add(grown);
        if requested > max_total {
            return Err(wasmtime::Error::msg(format!(
                "linear memory limit exceeded: {requested} bytes requested, limit is {max_total} bytes"
            )));
        }
        self.total = requested;
        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::{Engine, Module, Store};

    /// Two linear memories in one store, each growing to one page.
    const TWO_MEMORIES: &str = r#"
        (module
          (memory (export "a") 0)
          (memory (export "b") 0)
          (func (export "grow_a") (result i32) (memory.grow 0 (i32.const 1)))
          (func (export "grow_b") (result i32) (memory.grow 1 (i32.const 1)))
        )
    "#;

    fn store(max_total: Option<u64>) -> (Store<StoreMemoryLimiter>, Module) {
        let engine = Engine::default();
        let module = Module::new(&engine, wat::parse_str(TWO_MEMORIES).unwrap()).unwrap();
        let mut store = Store::new(&engine, StoreMemoryLimiter::new(max_total));
        store.limiter(|limiter| limiter);
        (store, module)
    }

    /// `memory` bounds a store's linear memories in total. `StoreLimitsBuilder::memory_size`
    /// would cap each one separately, letting a component with several core instances hold a
    /// multiple of what the operator configured.
    #[test]
    fn the_limit_must_count_a_stores_memories_in_total() {
        const PAGE: u64 = 64 * 1024;
        let (mut store, module) = store(Some(PAGE));
        let instance = wasmtime::Linker::new(store.engine())
            .instantiate(&mut store, &module)
            .unwrap();
        let grow_a = instance
            .get_typed_func::<(), i32>(&mut store, "grow_a")
            .unwrap();
        let grow_b = instance
            .get_typed_func::<(), i32>(&mut store, "grow_b")
            .unwrap();
        assert_eq!(0, grow_a.call(&mut store, ()).unwrap());
        // The second memory's own first page is already over the store's total.
        let err = grow_b.call(&mut store, ()).unwrap_err();
        assert!(
            format!("{err:?}").contains("linear memory limit exceeded"),
            "{err:?}"
        );
    }

    /// Exceeding the cap traps rather than returning -1 from `memory.grow`: a catchable
    /// allocation failure is a nondeterminism source for a workflow, since a guest that handles
    /// it can diverge on replay.
    #[test]
    fn exceeding_the_limit_must_trap_rather_than_fail_the_grow() {
        let (mut store, module) = store(Some(0));
        let instance = wasmtime::Linker::new(store.engine())
            .instantiate(&mut store, &module)
            .unwrap();
        let grow_a = instance
            .get_typed_func::<(), i32>(&mut store, "grow_a")
            .unwrap();
        assert!(
            grow_a.call(&mut store, ()).is_err(),
            "the guest must not observe a -1 it could handle"
        );
    }

    #[test]
    fn an_unlimited_cell_must_not_bound_a_store() {
        let (mut store, module) = store(None);
        let instance = wasmtime::Linker::new(store.engine())
            .instantiate(&mut store, &module)
            .unwrap();
        for name in ["grow_a", "grow_b"] {
            let grow = instance
                .get_typed_func::<(), i32>(&mut store, name)
                .unwrap();
            assert_eq!(0, grow.call(&mut store, ()).unwrap());
        }
    }
}
