fn main() {
    tonic_build::configure()
        .compile(&["proto/proximo.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
    println!("cargo:rerun-if-changed=proto");
}
