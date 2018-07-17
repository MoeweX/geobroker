package de.hasenburg.geofencebroker.communication;

import java.util.Objects;
import de.hasenburg.geofencebroker.exceptions.RouterException;

/**
 * RouterMessages contain the five frames of each valid message processed by the Router, excluding the empty delimiter.
 * 
 * @author jonathanhasenburg
 *
 */
public class RouterMessage {

	private String identity;

	private ControlPacketType controlPacketType;

	private String topic;

	private String geofence;

	private String payload;

	public RouterMessage(String identity,
						 ControlPacketType controlPacketType, String topic, String geofence, String payload) {

		this.identity = identity;
		this.controlPacketType = controlPacketType;
		this.topic = topic;
		this.geofence = geofence;
		this.payload = payload;
	}

	public static RouterMessage build(byte[][] messageArray) throws RouterException {

		if (messageArray.length != 6 ) {
			throw new RouterException("Could not create RouterMessage as message array needs 6 fields but has " + messageArray.length);
		}

		try {
			String identity = new String(messageArray[0]);
			ControlPacketType controlPacketType = ControlPacketType.valueOf(new String(messageArray[2])); // 1 is empty delimiter
			String topic = new String(messageArray[3]);
			String geofence = new String(messageArray[4]);
			String payload = new String(messageArray[5]);
			return new RouterMessage(identity, controlPacketType, topic, geofence, payload);
		} catch(Exception e) {
			throw new RouterException("Exception while converting message array content to RouterMessage", e);
		}

	}

	// ************************************************************
	// Generated Code
	// ************************************************************

	@Override
	public String toString() {
		return "RouterMessage{" +
				"identity='" + identity + '\'' +
				", controlPacketType=" + controlPacketType +
				", topic='" + topic + '\'' +
				", geofence='" + geofence + '\'' +
				", payload='" + payload + '\'' +
				'}';
	}

	public String getIdentity() {
		return identity;
	}

	public void setIdentity(String identity) {
		this.identity = identity;
	}

	public ControlPacketType getControlPacketType() {
		return controlPacketType;
	}

	public void setControlPacketType(ControlPacketType controlPacketType) {
		this.controlPacketType = controlPacketType;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getGeofence() {
		return geofence;
	}

	public void setGeofence(String geofence) {
		this.geofence = geofence;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
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
		return Objects.equals(getIdentity(), that.getIdentity()) &&
				getControlPacketType() == that.getControlPacketType() &&
				Objects.equals(getTopic(), that.getTopic()) &&
				Objects.equals(getGeofence(), that.getGeofence()) &&
				Objects.equals(getPayload(), that.getPayload());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getIdentity(), getControlPacketType(), getTopic(), getGeofence(), getPayload());
	}
}
