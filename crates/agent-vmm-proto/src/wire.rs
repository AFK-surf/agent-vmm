//! Preserve the signed service header's unknown-field check before prost discards fields.
use crate::service::v1::OpenServiceForwardRequest;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use std::sync::LazyLock;
use vmm_rpc::{Body, Status, StreamExt, Streaming};

pub(crate) fn service_stream(body: Body) -> Streaming<OpenServiceForwardRequest> {
    let mut first = true;
    Streaming::new(body.map(move |frame| {
        let bytes = frame?;
        if std::mem::take(&mut first) {
            checked_header(&bytes)
        } else {
            OpenServiceForwardRequest::decode(bytes)
                .map_err(|_| Status::invalid_argument("invalid service message"))
        }
    }))
}
fn checked_header<U: Message + Default>(bytes: &[u8]) -> Result<U, Status> {
    static HEADER: LazyLock<MessageDescriptor> = LazyLock::new(|| {
        DescriptorPool::decode(crate::FILE_DESCRIPTOR_SET)
            .unwrap()
            .get_message_by_name("agentvmm.service.v1.OpenServiceForwardRequest")
            .unwrap()
    });
    let invalid = || Status::invalid_argument("invalid service header");
    let original = DynamicMessage::decode(HEADER.clone(), bytes).map_err(|_| invalid())?;
    let request = U::decode(bytes).map_err(|_| invalid())?;
    let mut known = DynamicMessage::new(HEADER.clone());
    known.transcode_from(&request).map_err(|_| invalid())?;
    if original != known {
        return Err(Status::invalid_argument("unknown field in service header"));
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_signed_field_is_rejected_before_typed_decoding() {
        // Forward request field 1 is the header, whose field 6 is the capability.
        let frame = [0x0a, 5, 0x32, 3, 0xa8, 0x01, 1];
        assert!(OpenServiceForwardRequest::decode(&frame[..]).is_ok());
        assert_eq!(
            checked_header::<OpenServiceForwardRequest>(&frame)
                .unwrap_err()
                .code(),
            vmm_rpc::Code::InvalidArgument
        );
    }
}
