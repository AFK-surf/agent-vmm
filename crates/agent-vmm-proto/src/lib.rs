#![allow(clippy::large_enum_variant)]

pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/agent-vmm.bin"));

mod wire;

pub mod appliance {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/agentvmm.appliance.v1.rs"));
    }
}

pub mod host {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/agentvmm.host.v1.rs"));
    }
}

pub mod remote {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/agentvmm.remote.v1.rs"));
    }
}

pub mod service {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/agentvmm.service.v1.rs"));
    }
}

pub mod trust {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/agentvmm.trust.v1.rs"));
    }
}

pub mod google {
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
}
