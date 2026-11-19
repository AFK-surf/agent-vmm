//! Preserve the signed service header's unknown-field check before prost discards fields.
use crate::service::v1::OpenServiceForwardRequest;
use prost::{Message, bytes::Buf};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use std::{any::TypeId, sync::LazyLock};
use tonic::{
    Status,
    codec::{Codec, DecodeBuf, Decoder},
};
use tonic_prost::ProstCodec;

pub(crate) struct WireCodec<T, U>(ProstCodec<T, U>);
impl<T, U> Default for WireCodec<T, U> {
    fn default() -> Self {
        Self(ProstCodec::default())
    }
}
impl<T, U> Codec for WireCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = <ProstCodec<T, U> as Codec>::Encoder;
    type Decoder = WireDecoder<<ProstCodec<T, U> as Codec>::Decoder>;
    fn encoder(&mut self) -> Self::Encoder {
        self.0.encoder()
    }
    fn decoder(&mut self) -> Self::Decoder {
        WireDecoder {
            inner: self.0.decoder(),
            check_header: TypeId::of::<U>() == TypeId::of::<OpenServiceForwardRequest>(),
        }
    }
}
pub(crate) struct WireDecoder<D> {
    inner: D,
    check_header: bool,
}
impl<D, U> Decoder for WireDecoder<D>
where
    D: Decoder<Item = U, Error = Status>,
    U: Message + Default,
{
    type Item = U;
    type Error = Status;
    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<U>, Status> {
        if !std::mem::take(&mut self.check_header) {
            return self.inner.decode(buf);
        }
        checked_header(buf.copy_to_bytes(buf.remaining()).as_ref()).map(Some)
    }
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
            tonic::Code::InvalidArgument
        );
    }
}
