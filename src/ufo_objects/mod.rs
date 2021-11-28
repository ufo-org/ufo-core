use std::sync::Arc;

use super::errors::*;
use super::mmap_wrapers::*;

mod ufo_chunk;
mod ufo_config;
mod ufo_file_writeback;
mod ufo_id;
mod ufo_object;
mod ufo_offset;

pub(crate) use ufo_chunk::*;
pub use ufo_config::*;
pub(crate) use ufo_file_writeback::*;
pub use ufo_id::*;
pub use ufo_object::*;
pub(crate) use ufo_offset::*;

type DataHash = blake3::Hash;

pub fn hash_function(data: &[u8]) -> DataHash {
    #[cfg(feature = "parallel_hashing")]
    {
        if data.len() > 128 * 1024 {
            // On large blocks we can get significant gains from parallelism
            blake3::Hasher::new().update_rayon(data).finalize()
        } else {
            blake3::hash(data)
        }
    }

    #[cfg(not(feature = "parallel_hashing"))]
    {
        blake3::hash(data)
    }
}
