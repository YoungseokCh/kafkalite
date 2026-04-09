fn main() {
    let proto_file = "proto/kafkalite.proto";
    println!("cargo:rerun-if-changed={proto_file}");

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("failed to vend protoc");
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&[proto_file], &["proto"])
        .expect("failed to compile kafkalite protobufs");
}
