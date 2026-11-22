use std::future::Future;

use tokio::runtime::Builder;

/// Runs an async test body to completion on a current-thread runtime with timers and I/O, as
/// `#[tokio::test]` would.
///
/// # Panics
///
/// Panics when the runtime can't be built, which fails the test.
pub fn block_on<F: Future>(future: F) -> F::Output {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test runtime should build")
        .block_on(future)
}
