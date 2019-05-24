package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.exceptions.CommunicatorException;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.KryoSerializer;
import de.hasenburg.geobroker.commons.model.message.payloads.AbstractPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.Objects;
import java.util.Optional;

/**
 * Used for communication to other brokers and for the other brokers direct replies. Note, that other broker receive
 * these messages via the {@link ZMQProcess_Server}, so they get an {@link InternalServerMessage}.
 */
public class InternalBrokerMessage {

	private static final Logger logger = LogManager.getLogger();

	private ControlPacketType controlPacketType;
	private AbstractPayload payload;

	/**
	 * Optional is empty when - ZMsg is not a InternalBrokerMessage or null - Payload incompatible to control packet
	 * type - Payload misses fields
	 */
	public static Optional<InternalBrokerMessage> buildMessage(ZMsg msg, KryoSerializer kryo) {
		if (msg == null) {
			// happens when queue is empty
			return Optional.empty();
		}

		if (msg.size() != 2) {
			logger.error("Cannot parse message {} to InternalBrokerMessage, has wrong size.", msg.toString());
			return Optional.empty();
		}

		InternalBrokerMessage message = new InternalBrokerMessage();

		try {
			message.controlPacketType = ControlPacketType.valueOf(msg.popString());
			InternalBrokerMessage.validateControlPacketType(message.controlPacketType);
			byte[] arr = msg.pop().getData();
			message.payload = (AbstractPayload) kryo.read(arr, message.controlPacketType);
			if(message.payload == null){
				message = null;
			}
		} catch (Exception e) {
			logger.warn("Cannot parse message, due to exception, discarding it", e);
			message = null;
		}

		return Optional.ofNullable(message);
	}

	/**
	 * Validates whether the received message has a valid {@link ControlPacketType}.
	 *
	 * @throws CommunicatorException if the given type is not valid
	 */
	private static void validateControlPacketType(ControlPacketType givenType) throws CommunicatorException {
		if (givenType != ControlPacketType.BrokerForwardPublish) {
			throw new CommunicatorException(
					"ControlPacketType " + givenType.name() + " is not valid for InternalBrokerMessages");
		}
	}

	private InternalBrokerMessage() {
		// used for static buildMessage method
	}

	public InternalBrokerMessage(ControlPacketType controlPacketType, AbstractPayload payload) {
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
		return "InternalBrokerMessage{" + "controlPacketType=" + controlPacketType + ", payload=" + payload + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		InternalBrokerMessage that = (InternalBrokerMessage) o;
		return controlPacketType == that.controlPacketType && Objects.equals(payload, that.payload);
	}

	@Override
	public int hashCode() {
		return Objects.hash(controlPacketType, payload);
	}
}
