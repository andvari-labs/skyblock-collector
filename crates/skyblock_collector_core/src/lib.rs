//! The logic behind `skyblock-collector`, apart from its binary so the binary's command-line and
//! runtime dependencies stay out of it.

pub mod archive;
pub mod collect;
pub mod config;
pub mod db;
pub mod feed;
pub mod http;
pub mod ingest;
pub mod model;
pub mod nbt;
pub mod replay;
pub mod spool;
pub mod status;
