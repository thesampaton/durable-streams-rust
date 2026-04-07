use super::{AppendBuilder, CloseBuilder, CreateBuilder, ReadBuilder, StreamHandle, Subscription};
use crate::error::Error;
use crate::model::{
    AppendRequest, AppendResponse, CloseStreamRequest, CloseStreamResponse, ConnectRequest,
    ConnectResponse, CreateStreamRequest, CreateStreamResponse, DeleteRequest, DeleteResponse,
    HeadRequest, HeadResponse, LiveMode, ReadRequest, ReadResponse, RequestOptions,
    SubscribeRequest,
};
use crate::types::{AppendOutcome, Offset, StreamInfo};
use bytes::Bytes;
use serde::Serialize;
use std::borrow::ToOwned;

impl StreamHandle {
    /// Return the stream path bound to this handle.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Start building a stream creation request.
    ///
    /// If neither the builder nor the client config supplies a content type,
    /// [`CreateBuilder::send`] falls back to `application/octet-stream`.
    /// Use [`raw::CreateStreamRequest`](crate::raw::CreateStreamRequest) when
    /// you want protocol-shaped request construction with fully explicit
    /// fields.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// orders
    ///     .create()
    ///     .content_type("application/json")
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn create(&self) -> CreateBuilder {
        CreateBuilder {
            stream: self.clone(),
            content_type: self.client.default_content_type().map(ToOwned::to_owned),
            ttl_seconds: None,
            expires_at: None,
            closed: false,
            body: None,
            options: RequestOptions::default(),
        }
    }

    /// Fetch stream metadata.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// let info = orders.head().await?;
    /// println!("{:?}", info.content_type);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn head(&self) -> Result<StreamInfo, Error> {
        let response = self
            .client
            .head_raw(&self.path, &HeadRequest::default())
            .await?;
        Ok(StreamInfo::from(response))
    }

    /// Append raw bytes to this stream.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// orders
    ///     .append("hello world")
    ///     .content_type("text/plain")
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn append(&self, body: impl Into<Bytes>) -> AppendBuilder {
        AppendBuilder {
            stream: self.clone(),
            body: body.into(),
            content_type: self.client.default_content_type().map(ToOwned::to_owned),
            expected_seq: None,
            options: RequestOptions::default(),
        }
    }

    /// Serialize one JSON value and append it with `application/json`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// orders
    ///     .append_json(&serde_json::json!({ "type": "created" }))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn append_json<T>(&self, value: &T) -> Result<AppendOutcome, Error>
    where
        T: Serialize,
    {
        let body = serde_json::to_vec(value)?;
        self.append(body)
            .content_type("application/json")
            .send()
            .await
    }

    /// Start building a close request.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// let close = orders.close().send().await?;
    /// println!("{}", close.final_offset);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn close(&self) -> CloseBuilder {
        CloseBuilder {
            stream: self.clone(),
            body: None,
            content_type: self.client.default_content_type().map(ToOwned::to_owned),
            options: RequestOptions::default(),
        }
    }

    /// Start building a collected read request.
    ///
    /// The ergonomic builder defaults to `Offset::Beginning` and
    /// `LiveMode::CatchUp`, so `stream.read().send().await?` reads the stream
    /// from the start in the common case. Use [`ReadBuilder::offset`] and
    /// [`ReadBuilder::live`] only when you need non-default behavior.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// let page = orders.read().send().await?;
    ///
    /// println!("{}", page.next_offset);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn read(&self) -> ReadBuilder {
        ReadBuilder {
            stream: self.clone(),
            offset: Offset::Beginning,
            live: LiveMode::CatchUp,
            timeout: None,
            max_chunks: None,
            wait_for_up_to_date: false,
            cursor: None,
            if_none_match: None,
            options: RequestOptions::default(),
        }
    }

    /// Delete this stream.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use durable_streams_client::{Client, ClientConfig};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), durable_streams_client::Error> {
    /// let client = Client::new(ClientConfig::default())?;
    /// let orders = client.stream("/orders");
    ///
    /// orders.delete().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&self) -> Result<(), Error> {
        self.client
            .delete_raw(&self.path, &DeleteRequest::default())
            .await?;
        Ok(())
    }

    /// Create this stream using the protocol-shaped raw request API.
    pub async fn create_raw(
        &self,
        request: &CreateStreamRequest,
    ) -> Result<CreateStreamResponse, Error> {
        self.client.create_raw(&self.path, request).await
    }

    /// Fetch metadata for this stream using the protocol-shaped raw API.
    pub async fn connect_raw(&self, request: &ConnectRequest) -> Result<ConnectResponse, Error> {
        self.client.connect_raw(&self.path, request).await
    }

    /// Append to this stream using the protocol-shaped raw API.
    pub async fn append_raw(&self, request: &AppendRequest) -> Result<AppendResponse, Error> {
        self.client.append_raw(&self.path, request).await
    }

    /// Read from this stream using the protocol-shaped raw API.
    pub async fn read_raw(&self, request: &ReadRequest) -> Result<ReadResponse, Error> {
        self.client.read_raw(&self.path, request).await
    }

    /// Close this stream using the protocol-shaped raw API.
    pub async fn close_raw(
        &self,
        request: &CloseStreamRequest,
    ) -> Result<CloseStreamResponse, Error> {
        self.client.close_raw(&self.path, request).await
    }

    /// Fetch metadata for this stream using a raw HEAD request.
    pub async fn head_raw(&self, request: &HeadRequest) -> Result<HeadResponse, Error> {
        self.client.head_raw(&self.path, request).await
    }

    /// Delete this stream using the protocol-shaped raw API.
    pub async fn delete_raw(&self, request: &DeleteRequest) -> Result<DeleteResponse, Error> {
        self.client.delete_raw(&self.path, request).await
    }

    /// Start a background subscription using the raw read request model.
    #[must_use]
    pub fn subscribe_raw(&self, request: SubscribeRequest) -> Subscription {
        self.client.subscribe_raw(&self.path, request)
    }
}
