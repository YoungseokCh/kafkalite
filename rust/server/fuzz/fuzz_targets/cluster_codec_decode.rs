#![no_main]

use kafkalite_server::cluster::codec::{decode_request, decode_response};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 65_536 {
        return;
    }

    let _ = decode_request(data);
    let _ = decode_response(data);
});
