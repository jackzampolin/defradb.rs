# FetcherWrapper Fat Pointer Transmute

**Severity**: Medium
**Category**: Unsafe Code — Lifetime Erasure via Raw Pointers
**Status**: Sound but fragile

## Summary

`FetcherWrapper` in `crates/query/src/runner/fetcher.rs:32-73` decomposes a `*const dyn DocFetcher` fat pointer into `(data_ptr, vtable)` via `transmute`, stores them as raw `*const ()` pointers, and reconstructs the fat pointer on each method call. This erases the lifetime of the original reference. Safety depends entirely on the caller ensuring the original reference outlives all uses of the wrapper.

## Affected Files

- `crates/query/src/runner/fetcher.rs:32-73`
- Usage sites: `crates/query/src/runner/query/nested.rs:31`, `crates/query/src/runner/query/aggregate.rs:263`, `crates/query/src/runner/explain/execute.rs:251`

## Details

### The Pattern

```rust
pub(crate) struct FetcherWrapper {
    data_ptr: *const (),
    vtable: *const (),
    _phantom: PhantomData<*const dyn DocFetcher>,
}

impl FetcherWrapper {
    pub(crate) fn new(fetcher: &dyn DocFetcher) -> Self {
        let ptr = fetcher as *const dyn DocFetcher;
        let (data_ptr, vtable) =
            unsafe { std::mem::transmute::<*const dyn DocFetcher, (*const (), *const ())>(ptr) };
        Self { data_ptr, vtable, _phantom: PhantomData }
    }

    fn get_fetcher(&self) -> &dyn DocFetcher {
        let ptr = unsafe {
            std::mem::transmute::<(*const (), *const ()), *const dyn DocFetcher>((
                self.data_ptr, self.vtable,
            ))
        };
        unsafe { &*ptr }
    }
}

unsafe impl Send for FetcherWrapper {}
unsafe impl Sync for FetcherWrapper {}
```

### Fat Pointer Layout Assumption

The code assumes `*const dyn Trait` has the layout `(data_ptr, vtable_ptr)`. This is the de facto standard layout in rustc and is documented in the Rustonomicon, but it is **not formally stabilized**. The `std::ptr::metadata` API (stabilization in progress) would provide a safe alternative.

### Lifetime Safety Analysis

The wrapper is created in three call sites:

1. **`execute_nested_select_with_planner`** (nested.rs:31): `fetcher: &dyn DocFetcher` → `FetcherWrapper::new(fetcher)` → wrapped in `Arc::new(fetcher_arc)` → passed to `Planner`. The planner executes within the same function scope, and `plan.close().await` is called before the function returns. The `fetcher` reference (from the method parameter) is guaranteed alive for the entire function body. **Sound.**

2. **`execute_top_level_aggregate`** (aggregate.rs:263): Same pattern as above — fetcher ref lives for the function scope, wrapper consumed within the same scope. **Sound.**

3. **`execute_explain_with_vars`** (execute.rs:251): Same pattern. **Sound.**

In all three cases, the `FetcherWrapper` is:
- Created from a `&dyn DocFetcher` parameter
- Wrapped in `Arc::new()`
- Passed to a `Planner`
- Consumed within the same async function before the function returns

The lifetime invariant holds because the wrapper never escapes the function that created it.

### Send+Sync Analysis

`DocFetcher` requires `MaybeSendSync`, which on non-WASM targets resolves to `Send + Sync`. Since the underlying data is Send+Sync, and the wrapper only holds pointers to it, the manual Send+Sync impls are sound.

### Risk: No Compile-Time Enforcement

The `PhantomData<*const dyn DocFetcher>` does NOT enforce the lifetime — it uses a raw pointer phantom, which has no lifetime parameter. A future refactor could accidentally:
- Store the `Arc<FetcherWrapper>` in a struct that outlives the fetcher
- Return it from a function
- Move it to a spawned task

Any of these would create a dangling pointer that the compiler cannot catch.

## Remediation

**Medium priority.** The code is currently sound but the safety margin is thin.

1. **Best**: When `std::ptr::metadata` stabilizes, use `Pointee::Metadata` instead of transmute to decompose/reconstruct the fat pointer.
2. **Good**: Add a lifetime parameter to FetcherWrapper: `FetcherWrapper<'a>` with `PhantomData<&'a dyn DocFetcher>`. This would make the borrow checker enforce the lifetime. The caller sites would need to adjust but this is the proper fix.
3. **Minimum**: Add a safety comment at each usage site documenting why the lifetime is upheld, and add `#[must_not_store]` or equivalent doc warnings.

## Test Gap

- No test specifically exercises the fat pointer round-trip under Miri (the wrapper is used in integration tests but Miri can't run them due to async + IO).
- No test verifies that the reconstructed vtable matches the original.
- A unit test could create a `FetcherWrapper`, call `get_fetcher()`, and verify the result matches the original reference (pointer equality).
