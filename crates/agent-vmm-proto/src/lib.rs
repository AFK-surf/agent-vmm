#![allow(clippy::large_enum_variant)]

pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/agent-vmm.bin"));

pub mod appliance {
    pub mod v1 {
        tonic::include_proto!("agentvmm.appliance.v1");
    }
}

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

pub mod google {
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
}
