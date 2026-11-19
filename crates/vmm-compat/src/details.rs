use prost::Message;
use std::collections::HashMap;

#[derive(Clone, PartialEq, Message)]
pub struct ErrorInfo {
    #[prost(string, tag = "1")]
    pub reason: String,
    #[prost(string, tag = "2")]
    pub domain: String,
    #[prost(map = "string, string", tag = "3")]
    pub metadata: HashMap<String, String>,
}
#[derive(Clone, PartialEq, Message)]
pub struct ResourceInfo {
    #[prost(string, tag = "1")]
    pub resource_type: String,
    #[prost(string, tag = "2")]
    pub resource_name: String,
    #[prost(string, tag = "3")]
    pub owner: String,
    #[prost(string, tag = "4")]
    pub description: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct RetryInfo {
    #[prost(message, optional, tag = "1")]
    pub retry_delay: Option<prost_types::Duration>,
}
#[derive(Clone, PartialEq, Message)]
pub struct BadRequest {
    #[prost(message, repeated, tag = "1")]
    pub field_violations: Vec<FieldViolation>,
}
#[derive(Clone, PartialEq, Message)]
pub struct FieldViolation {
    #[prost(string, tag = "1")]
    pub field: String,
    #[prost(string, tag = "2")]
    pub description: String,
}
