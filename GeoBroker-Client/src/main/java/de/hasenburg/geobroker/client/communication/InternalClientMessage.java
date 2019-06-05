package de.hasenburg.geobroker.client.communication;

import de.hasenburg.geobroker.commons.model.KryoSerializer;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.payloads.AbstractPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.zeromq.ZMsg;

import java.util.Objects;

public class InternalClientMessage {

	private static final Logger logger = LogManager.getLogger();

	private ControlPacketType controlPacketType;
	private AbstractPayload payload;

	/**
	 * Returns null when:
	 * 	- ZMsg is not a InternalClientMessage or null
	 * 	- Payload incompatible to control packet type
	 * 	- Payload misses fields
	 */
	public static @Nullable InternalClientMessage buildMessage(ZMsg msg, KryoSerializer kryo) {
		if (msg == null) {
			// happens when queue is empty
			return null;
		}

		if (msg.size() != 2) {
			logger.error("Cannot parse message {} to InternalClientMessage, has wrong size.", msg.toString());
			return null;
		}

		InternalClientMessage message = new InternalClientMessage();

		try {
			message.controlPacketType = ControlPacketType.valueOf(msg.popString());
			byte[] arr = msg.pop().getData();
			message.payload = kryo.read(arr, message.controlPacketType);
			if(message.payload == null){
				message = null;
			}
		} catch (Exception e) {
			logger.warn("Cannot parse message, due to exception, discarding it", e);
			message = null;
		}
		return message;
	}

	private InternalClientMessage() {

	}

	public InternalClientMessage(ControlPacketType controlPacketType, AbstractPayload payload) {
		this.controlPacketType = controlPacketType;
		this.payload = payload;
	}

	public ZMsg getZMsg(KryoSerializer kryo) {
		byte[] arr = kryo.write(payload);
		return ZMsg.newStringMsg(controlPacketType.name()).addLast(arr);
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
