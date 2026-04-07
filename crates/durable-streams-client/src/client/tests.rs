use super::Client;
use crate::{LiveMode, Offset};

#[test]
fn create_builder_uses_client_default_content_type() {
    let client = Client::builder()
        .default_content_type("application/json")
        .build()
        .expect("client builds");
    let request = client.stream("/orders").create().into_raw();

    assert_eq!(request.content_type, "application/json");
}

#[test]
fn append_builder_maps_expected_seq_to_raw_stream_seq() {
    let client = Client::builder().build().expect("client builds");
    let request = client
        .stream("/orders")
        .append("payload")
        .expected_seq("42-0")
        .into_raw();

    assert_eq!(request.stream_seq.as_deref(), Some("42-0"));
}

#[test]
fn read_builder_uses_typed_offsets() {
    let client = Client::builder().build().expect("client builds");
    let request = client
        .stream("/orders")
        .read()
        .offset(Offset::Now)
        .live(LiveMode::Auto)
        .until_up_to_date()
        .to_raw_request();

    assert_eq!(request.offset.as_deref(), Some("now"));
    assert_eq!(request.live, LiveMode::Auto);
    assert!(request.wait_for_up_to_date);
}
