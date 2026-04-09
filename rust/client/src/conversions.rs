use crate::proto;
use crate::{AppendAck, Header, NewMessage, StoredMessage};

impl From<Header> for proto::Header {
    fn from(value: Header) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

impl From<proto::Header> for Header {
    fn from(value: proto::Header) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}

impl From<NewMessage> for proto::NewMessage {
    fn from(value: NewMessage) -> Self {
        Self {
            topic: value.topic,
            key: value.key.unwrap_or_default(),
            payload_json: value.payload_json,
            headers: value.headers.into_iter().map(Into::into).collect(),
            published_at_unix_ms: value.published_at_unix_ms,
        }
    }
}

impl From<proto::AppendAck> for AppendAck {
    fn from(value: proto::AppendAck) -> Self {
        Self {
            topic: value.topic,
            partition: value.partition,
            offset: value.offset,
            published_at_unix_ms: value.published_at_unix_ms,
        }
    }
}

impl From<StoredMessage> for proto::StoredMessage {
    fn from(value: StoredMessage) -> Self {
        Self {
            topic: value.topic,
            partition: value.partition,
            offset: value.offset,
            key: value.key.unwrap_or_default(),
            payload_json: value.payload_json,
            headers: value.headers.into_iter().map(Into::into).collect(),
            published_at_unix_ms: value.published_at_unix_ms,
        }
    }
}

impl From<proto::StoredMessage> for StoredMessage {
    fn from(value: proto::StoredMessage) -> Self {
        Self {
            topic: value.topic,
            partition: value.partition,
            offset: value.offset,
            key: if value.key.is_empty() {
                None
            } else {
                Some(value.key)
            },
            payload_json: value.payload_json,
            headers: value.headers.into_iter().map(Into::into).collect(),
            published_at_unix_ms: value.published_at_unix_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_conversion_preserves_raw_bytes() {
        let header = Header {
            key: "x-bin".to_string(),
            value: vec![0, 159, 146, 150],
        };

        let proto_header = proto::Header::from(header.clone());
        assert_eq!(proto_header.value, header.value);

        let roundtrip = Header::from(proto_header);
        assert_eq!(roundtrip, header);
    }
}
