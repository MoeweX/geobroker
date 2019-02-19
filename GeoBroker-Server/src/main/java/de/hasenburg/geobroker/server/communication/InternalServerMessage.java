package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.model.JSONable;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.model.message.payloads.AbstractPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Objects;
import java.util.Optional;

public class InternalServerMessage {

	private static final Logger logger = LogManager.getLogger();

	private String clientIdentifier;
	private ControlPacketType controlPacketType;
	private AbstractPayload payload;

	/**
	 * Optional is empty when
	 * 	- ZMsg is not a InternalClientMessage or null
	 * 	- Payload incompatible to control packet type
	 * 	- Payload misses fields
	 */
	public static Optional<InternalServerMessage> buildMessage(ZMsg msg) {
		if (msg == null) {
			// happens when queue is empty
			return Optional.empty();
		}

		if (msg.size() != 3) {
			logger.error("Cannot parse message {} to InternalServerMessage, has wrong size.", msg.toString());
			return Optional.empty();
		}

		InternalServerMessage message = new InternalServerMessage();

		try {
			message.clientIdentifier = msg.popString();
			message.controlPacketType = ControlPacketType.valueOf(msg.popString());
			String s = msg.pop().getString(ZMQ.CHARSET);
			message.payload = Utility.buildPayloadFromString(s, message.controlPacketType);
		} catch (Exception e) {
			logger.warn("Cannot parse message, due to exception, discarding it", e);
			message = null;
		}

		return Optional.ofNullable(message);
	}

	private InternalServerMessage() {

	}

	public InternalServerMessage(String clientIdentifier,
								 ControlPacketType controlPacketType, AbstractPayload payload) {
		this.clientIdentifier = clientIdentifier;
		this.controlPacketType = controlPacketType;
		this.payload = payload;
	}

	public ZMsg getZMsg() {
		return ZMsg.newStringMsg(clientIdentifier, controlPacketType.name(), JSONable.toJSON(payload));
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
		return "InternalServerMessage{" +
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
		if (!(o instanceof InternalServerMessage)) {
			return false;
		}
		InternalServerMessage that = (InternalServerMessage) o;
		return Objects.equals(getClientIdentifier(), that.getClientIdentifier()) &&
				getControlPacketType() == that.getControlPacketType() &&
				Objects.equals(getPayload(), that.getPayload());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getClientIdentifier(), getControlPacketType(), getPayload());
	}
}
