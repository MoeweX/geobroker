package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.geofence.Geofence;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.Objects;
import java.util.Optional;

public class RouterMessage {

	private static final Logger logger = LogManager.getLogger();

	private String clientIdentifier;
	private ControlPacketType controlPacketType;
	private Topic topic;
	private Geofence geofence;
	private Payload payload;

	public static Optional<RouterMessage> buildRouterMessage(ZMsg msg) {
		if (msg == null) {
			// happens when queue is empty
			return Optional.empty();
		}

		if (msg.size() != 5) {
			logger.error("Cannot parse message {} to RouterMessage, has wrong size.", msg.toString());
			return Optional.empty();
		}

		RouterMessage message = new RouterMessage();

		try {
			message.clientIdentifier = msg.popString();
			message.controlPacketType = ControlPacketType.valueOf(msg.popString());
			message.topic = new Topic(msg.popString());
			message.geofence = Geofence.fromJSON(msg.popString());
			String s = new String(msg.pop().getData());
			message.payload = Utility.buildPayloadFromString(s, message.controlPacketType);
		} catch (Exception e) {
			logger.error("Cannot parse message, due to exception.", e);
			message = null;
		}

		return Optional.ofNullable(message);
	}

	public RouterMessage() {

	}

	public RouterMessage(String clientIdentifier,
						 ControlPacketType controlPacketType) {
		this.clientIdentifier = clientIdentifier;
		this.controlPacketType = controlPacketType;
		this.topic = new Topic("");
		this.geofence = new Geofence();
		this.payload = new Payload();
	}

	public RouterMessage(String clientIdentifier,
						 ControlPacketType controlPacketType, Payload payload) {
		this.clientIdentifier = clientIdentifier;
		this.controlPacketType = controlPacketType;
		this.topic = new Topic("");
		this.geofence = new Geofence();
		this.payload = payload;
	}

	public RouterMessage(String clientIdentifier,
						 ControlPacketType controlPacketType, Topic topic, Geofence geofence, Payload payload) {
		this.clientIdentifier = clientIdentifier;
		this.controlPacketType = controlPacketType;
		this.topic = topic;
		this.geofence = geofence;
		this.payload = payload;
	}

	public ZMsg getZmsg() {
		ZMsg msg = ZMsg.newStringMsg(clientIdentifier, controlPacketType.name(), topic.getTopic(), geofence.toJSON());
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

	public Topic getTopic() {
		return topic;
	}

	public Geofence getGeofence() {
		return geofence;
	}

	public Payload getPayload() {
		return payload;
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
				Objects.equals(getTopic(), that.getTopic()) &&
				Objects.equals(getGeofence(), that.getGeofence()) &&
				Objects.equals(getPayload(), that.getPayload());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getClientIdentifier(), getControlPacketType(), getTopic(), getGeofence(), getPayload());
	}

	@Override
	public String toString() {
		return "RouterMessage{" +
				"clientIdentifier='" + clientIdentifier + '\'' +
				", controlPacketType=" + controlPacketType +
				", topic=" + topic +
				", geofence='" + geofence.toString() + '\'' +
				", payload='" + payload + '\'' +
				'}';
	}
}
