pub mod auth {
    tonic::include_proto!("auth");
}

pub mod bundle {
    tonic::include_proto!("bundle");
}

pub mod google {
    pub mod protobuf {
        tonic::include_proto!("google.protobuf");
    }
}

pub mod packet {
    tonic::include_proto!("packet");
}

pub mod searcher {
    tonic::include_proto!("searcher");
}

pub mod shared {
    tonic::include_proto!("shared");
}
