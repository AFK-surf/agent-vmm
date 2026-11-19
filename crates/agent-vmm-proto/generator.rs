use heck::ToSnakeCase;
use prost_build::{Method, Service, ServiceGenerator};
use std::fmt::Write;

pub struct Generator;

impl ServiceGenerator for Generator {
    fn generate(&mut self, service: Service, out: &mut String) {
        let name = &service.name;
        let module = name.to_snake_case();
        let full_name = format!("{}.{}", service.package, service.proto_name);
        // Keep the descriptor for stored messages but omit the retired tunnel binding.
        let methods: Vec<_> = service
            .methods
            .iter()
            .filter(|m| {
                !(full_name == "agentvmm.remote.v1.RemoteControllerService"
                    && m.proto_name == "ServeSession")
            })
            .collect();
        writeln!(
            out,
            "pub mod {module}_client {{
            use super::*;
            use vmm_rpc as rpc;
            #[derive(Clone)]
            pub struct {name}Client {{ channel: rpc::Client }}
            impl {name}Client {{
              pub fn new(channel: rpc::Client) -> Self {{ Self {{ channel }} }}"
        )
        .unwrap();
        for m in &methods {
            client(out, m, &full_name);
        }
        writeln!(out, "}} }}").unwrap();

        writeln!(
            out,
            "pub mod {module}_server {{
            use super::*;
            use vmm_rpc as rpc;
            use std::sync::Arc;
            #[rpc::async_trait]
            pub trait {name}: Send + Sync + 'static {{"
        )
        .unwrap();
        for m in &methods {
            declaration(out, m);
        }
        writeln!(out, "}}
            pub struct {name}Server<T>(Arc<T>);
            impl<T: {name}> {name}Server<T> {{
                pub fn new(service:T) -> Self {{ Self(Arc::new(service)) }}
                pub fn from_arc(service:Arc<T>) -> Self {{ Self(service) }}
            }}
            #[rpc::async_trait]
            impl<T: {name}> rpc::Service for {name}Server<T> {{
                fn name(&self) -> &'static str {{ \"{full_name}\" }}
                async fn call(&self, method: &str, request: rpc::Request<rpc::Body>) -> Result<rpc::Body, rpc::Status> {{
                    match method {{").unwrap();
        for m in &methods {
            dispatch(out, m);
        }
        writeln!(
            out,
            "_ => Err(rpc::Status::unimplemented(\"unknown RPC method\")) }} }} }} }}"
        )
        .unwrap();
    }
}

fn client(out: &mut String, m: &Method, service: &str) {
    let name = &m.name;
    let path = format!("/{service}/{}", m.proto_name);
    let (input, output) = (&m.input_type, &m.output_type);
    let (request, to_stream) = if m.client_streaming {
        ("IntoStreamingRequest", "request.into_streaming_request()")
    } else {
        (
            "IntoRequest",
            "request.into_request().map(|message| rpc::once(async move { message }))",
        )
    };
    let (response, result) = if m.server_streaming {
        (format!("rpc::Streaming<{output}>"), "Ok(response)")
    } else {
        (
            output.to_string(),
            "Ok(rpc::Response::new(response.into_inner().single().await?))",
        )
    };
    writeln!(out, "pub async fn {name}(&mut self, request: impl rpc::{request}<{input}>) -> Result<rpc::Response<{response}>, rpc::Status> {{
        let response = self.channel.call::<{input}, {output}, _>(\"{path}\", {to_stream}).await?;
        {result}
    }}").unwrap();
}

fn declaration(out: &mut String, m: &Method) {
    let mut input = m.input_type.clone();
    let mut output = m.output_type.clone();
    if m.client_streaming {
        input = format!("rpc::Streaming<{input}>");
    }
    if m.server_streaming {
        let assoc = format!("{}Stream", m.proto_name);
        writeln!(
            out,
            "type {assoc}: rpc::Stream<Item = Result<{output}, rpc::Status>> + Send + 'static;"
        )
        .unwrap();
        output = format!("Self::{assoc}");
    }
    writeln!(out, "async fn {}(&self, request: rpc::Request<{input}>) -> Result<rpc::Response<{output}>, rpc::Status>;", m.name).unwrap();
}

fn dispatch(out: &mut String, m: &Method) {
    let request = if m.input_proto_type == ".agentvmm.service.v1.OpenServiceForwardRequest" {
        "request.map(crate::wire::service_stream)"
    } else if m.client_streaming {
        "request.streaming()"
    } else {
        "request.unary().await?"
    };
    let body = if m.server_streaming {
        "rpc::Body::encode(response.into_inner())"
    } else {
        "response.unary_body()"
    };
    writeln!(
        out,
        "\"{}\" => {{ let response = self.0.{}({request}).await?; Ok({body}) }},",
        m.proto_name, m.name
    )
    .unwrap();
}
