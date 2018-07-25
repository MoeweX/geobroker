package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.payload.AbstractPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.Objects;
import java.util.Optional;

public class RouterMessage {

	private static final Logger logger = LogManager.getLogger();

	private String clientIdentifier;
	private ControlPacketType controlPacketType;
	private AbstractPayload payload;

	/**
	 * Optional is empty when
	 * 	- ZMsg is not a DealerMessage or null
	 * 	- Payload incompatible to control packet type
	 * 	- Payload misses fields
	 */
	public static Optional<RouterMessage> buildRouterMessage(ZMsg msg) {
		if (msg == null) {
			// happens when queue is empty
			return Optional.empty();
		}

		if (msg.size() != 3) {
			logger.error("Cannot parse message {} to RouterMessage, has wrong size.", msg.toString());
			return Optional.empty();
		}

		RouterMessage message = new RouterMessage();

		try {
			message.clientIdentifier = msg.popString();
			message.controlPacketType = ControlPacketType.valueOf(msg.popString());
			String s = new String(msg.pop().getData()); // TODO would be great to use String instead of byte[]
			message.payload = Utility.buildPayloadFromString(s, message.controlPacketType);
		} catch (Exception e) {
			logger.warn("Cannot parse message, due to exception, discarding it", e);
			message = null;
		}

		return Optional.ofNullable(message);
	}

	private RouterMessage() {

	}

	public RouterMessage(String clientIdentifier,
						 ControlPacketType controlPacketType, AbstractPayload payload) {
		this.clientIdentifier = clientIdentifier;
		this.controlPacketType = controlPacketType;
		this.payload = payload;
	}

	public ZMsg getZMsg() {
		ZMsg msg = ZMsg.newStringMsg(clientIdentifier, controlPacketType.name());
		msg.add(JSONable.toJSON(payload).getBytes()); // cannot just add string, encoding fails
		return msg;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	public String getClientIdentifier() {
		return clientIdentifier;
	}

	public ControlPacketType getControlPacketType() {
		return controlPacketType;
	}

	public AbstractPayload getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return "RouterMessage{" +
				"clientIdentifier='" + clientIdentifier + '\'' +
				", controlPacketType=" + controlPacketType +
				", payload=" + payload +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof RouterMessage)) {
			return false;
		}
		RouterMessage that = (RouterMessage) o;
		return Objects.equals(getClientIdentifier(), that.getClientIdentifier()) &&
				getControlPacketType() == that.getControlPacketType() &&
				Objects.equals(getPayload(), that.getPayload());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getClientIdentifier(), getControlPacketType(), getPayload());
	}
}
