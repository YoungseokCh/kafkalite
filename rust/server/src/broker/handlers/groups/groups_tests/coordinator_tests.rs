use kafka_protocol::messages::FindCoordinatorRequest;

use super::*;

#[test]
fn find_coordinator_v4_falls_back_to_single_key() {
    let broker = test_broker();

    let response = handle_find_coordinator(
        &broker,
        FindCoordinatorRequest::default().with_key(StrBytes::from("group-a".to_string())),
        4,
    );

    assert_eq!(response.coordinators.len(), 1);
    assert_eq!(response.coordinators[0].key.to_string(), "group-a");
}

#[test]
fn find_coordinator_v4_uses_explicit_keys_and_v3_uses_legacy_fields() {
    let broker = test_broker();

    let v4 = handle_find_coordinator(
        &broker,
        FindCoordinatorRequest::default()
            .with_key(StrBytes::from("ignored".to_string()))
            .with_coordinator_keys(vec![
                StrBytes::from("group-a".to_string()),
                StrBytes::from("group-b".to_string()),
            ]),
        4,
    );
    assert_eq!(v4.coordinators.len(), 2);
    assert_eq!(v4.coordinators[0].key.to_string(), "group-a");
    assert_eq!(v4.coordinators[1].key.to_string(), "group-b");

    let v3 = handle_find_coordinator(
        &broker,
        FindCoordinatorRequest::default()
            .with_key(StrBytes::from("group-c".to_string()))
            .with_coordinator_keys(vec![StrBytes::from("group-d".to_string())]),
        3,
    );
    assert_eq!(v3.error_code, 0);
    assert_eq!(v3.node_id, BrokerId(1));
    assert_eq!(v3.host.to_string(), "127.0.0.1");
    assert_eq!(v3.port, 9092);
    assert!(v3.coordinators.is_empty());
}
