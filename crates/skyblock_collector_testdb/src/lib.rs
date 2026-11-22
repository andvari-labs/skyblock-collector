//! Support for `skyblock-collector`'s tests: a throwaway Postgres server in a container, and a
//! runtime to run async tests on.
//!
//! Tests depend on this crate only as a dev-dependency.

mod block_on;
mod test_postgres;

pub use block_on::block_on;
pub use test_postgres::{TestPostgres, TestPostgresError};
