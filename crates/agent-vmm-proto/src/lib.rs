#![allow(clippy::large_enum_variant)]

pub mod host {
    pub mod v1 {
        tonic::include_proto!("agentvmm.host.v1");
    }
}

pub mod remote {
    pub mod v1 {
        tonic::include_proto!("agentvmm.remote.v1");
    }
}

pub mod service {
    pub mod v1 {
        tonic::include_proto!("agentvmm.service.v1");
    }
}

pub mod trust {
    pub mod v1 {
        tonic::include_proto!("agentvmm.trust.v1");
    }
}
