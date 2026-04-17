#![no_main]

use bytes::Bytes;
use kafkalite_server::protocol::peek_key_and_version;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut frame = Bytes::copy_from_slice(data);
    let _ = peek_key_and_version(&mut frame);
});
