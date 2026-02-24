use tonic_prost_build::configure;

fn main() {
    configure()
        .compile_well_known_types(true)
        .compile_protos(
            &[
                "protos/auth.proto",
                "protos/bundle.proto",
                "protos/packet.proto",
                "protos/searcher.proto",
                "protos/shared.proto",
            ],
            &["protos"],
        )
        .expect("failed to compile jito protobuf definitions");
}
