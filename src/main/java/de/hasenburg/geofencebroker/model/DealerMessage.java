package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.Objects;
import java.util.Optional;

public class DealerMessage {

	private static final Logger logger = LogManager.getLogger();

	private ControlPacketType controlPacketType;
	private Topic topic;
	private String geofence;
	private String payload;

	public static Optional<DealerMessage> buildDealerMessage(ZMsg msg) {
		if (msg == null) {
			// happens when queue is empty
			return Optional.empty();
		}

		if (msg.size() != 4) {
			logger.error("Cannot parse message {} to DealerMessage, has wrong size.", msg.toString());
			return Optional.empty();
		}

		DealerMessage message = new DealerMessage();

		try {
			message.controlPacketType = ControlPacketType.valueOf(msg.popString());
			message.topic = new Topic(msg.popString());
			message.geofence = msg.popString();
			message.payload = msg.popString();
		} catch (Exception e) {
			logger.error("Cannot parse message, due to exception.", e);
			message = null;
		}

		return Optional.ofNullable(message);
	}

	public DealerMessage() {

	}

	public DealerMessage(ControlPacketType controlPacketType) {
		this.controlPacketType = controlPacketType;
		this.topic = new Topic("");
		this.geofence = "";
		this.payload = "";
	}

	public DealerMessage(ControlPacketType controlPacketType, String payload) {
		this.controlPacketType = controlPacketType;
		this.topic = new Topic("");
		this.geofence = "";
		this.payload = payload;
	}

	public DealerMessage(ControlPacketType controlPacketType, Topic topic, String geofence, String payload) {
		this.controlPacketType = controlPacketType;
		this.topic = topic;
		this.geofence = geofence;
		this.payload = payload;
	}

	public ZMsg getZmsg() {
		return ZMsg.newStringMsg(controlPacketType.name(), topic.getTopic(), geofence, payload);
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	public ControlPacketType getControlPacketType() {
		return controlPacketType;
	}

	public Topic getTopic() {
		return topic;
	}

	public String getGeofence() {
		return geofence;
	}

	public String getPayload() {
		return payload;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof DealerMessage)) {
			return false;
		}
		DealerMessage that = (DealerMessage) o;
		return getControlPacketType() == that.getControlPacketType() &&
				Objects.equals(getTopic(), that.getTopic()) &&
				Objects.equals(getGeofence(), that.getGeofence()) &&
				Objects.equals(getPayload(), that.getPayload());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getControlPacketType(), getTopic(), getGeofence(), getPayload());
	}

	@Override
	public String toString() {
		return "DealerMessage{" +
				"controlPacketType=" + controlPacketType +
				", topic=" + topic +
				", geofence='" + geofence + '\'' +
				", payload='" + payload + '\'' +
				'}';
	}
}
