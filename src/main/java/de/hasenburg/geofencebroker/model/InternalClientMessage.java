package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.payload.AbstractPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Objects;
import java.util.Optional;

public class InternalClientMessage {

	private static final Logger logger = LogManager.getLogger();

	private ControlPacketType controlPacketType;
	private AbstractPayload payload;

	/**
	 * Optional is empty when
	 * 	- ZMsg is not a InternalClientMessage or null
	 * 	- Payload incompatible to control packet type
	 * 	- Payload misses fields
	 */
	public static Optional<InternalClientMessage> buildDealerMessage(ZMsg msg) {
		if (msg == null) {
			// happens when queue is empty
			return Optional.empty();
		}

		if (msg.size() != 2) {
			logger.error("Cannot parse message {} to InternalClientMessage, has wrong size.", msg.toString());
			return Optional.empty();
		}

		InternalClientMessage message = new InternalClientMessage();

		try {
			message.controlPacketType = ControlPacketType.valueOf(msg.popString());
			String s = msg.pop().getString(ZMQ.CHARSET);
			message.payload = Utility.buildPayloadFromString(s, message.controlPacketType);
		} catch (Exception e) {
			logger.warn("Cannot parse message, due to exception, discarding it", e);
			message = null;
		}

		return Optional.ofNullable(message);
	}

	private InternalClientMessage() {

	}

	public InternalClientMessage(ControlPacketType controlPacketType, AbstractPayload payload) {
		this.controlPacketType = controlPacketType;
		this.payload = payload;
	}

	public ZMsg getZMsg() {
		return ZMsg.newStringMsg(controlPacketType.name(), JSONable.toJSON(payload));
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	public ControlPacketType getControlPacketType() {
		return controlPacketType;
	}

	public AbstractPayload getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return "InternalClientMessage{" +
				"controlPacketType=" + controlPacketType +
				", payload=" + payload +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof InternalClientMessage)) {
			return false;
		}
		InternalClientMessage that = (InternalClientMessage) o;
		return getControlPacketType() == that.getControlPacketType() &&
				Objects.equals(getPayload(), that.getPayload());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getControlPacketType(), getPayload());
	}
}
