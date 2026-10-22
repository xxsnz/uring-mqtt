mod broker;
mod codec;
mod connection;
mod error;

#[cfg(any(test, fuzzing))]
#[doc(hidden)]
pub use broker::fuzz;
pub use broker::{
    BrokerConfig, BrokerHandle, DroppedPublishesHandle, MaxInboundPacketSize, MqttBroker, Publish,
    PublishCallback, QoS, ShutdownHandle,
};
pub use connection::ConnectionState;
pub use error::Error;

#[cfg(feature = "runtime-seam")]
#[doc(hidden)]
pub mod runtime_seam {
    pub use crate::broker::handler::{serve_session, SessionOutcome};
    pub use crate::broker::handshake::MAX_TIMEOUT_SECS;
    pub use crate::broker::runtime::{HandshakeIo, PacketIo, TimedOut, Timer};
    pub use crate::broker::worker::{event_channel, EventReceiver, EventSender};
    pub use crate::codec::mqtt::{MqttDecoder, MqttEncoder, MqttPacket};
    pub use crate::codec::version::{ProtocolVersion, VersionDecoder};
    pub use crate::error::{is_routine_disconnect, is_truncated_packet};
}
