package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import javax.swing.text.html.Option;
import java.util.Objects;
import java.util.Optional;

public class DealerMessage {

	private static final Logger logger = LogManager.getLogger();

	private ControlPacketType controlPacketType;
	private Topic topic;
	private String geofence;
	private Payload payload;

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
			String s = new String(msg.pop().getData());
			switch (message.controlPacketType) {
				case PINGREQ:
					message.payload = JSONable.fromJSON(s, PayloadPINGREQ.class).orElseGet(PayloadPINGREQ::new);
					break;
				default:
					message.payload = JSONable.fromJSON(s, Payload.class).orElseGet(Payload::new);
			}
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
		this.payload = new Payload();
	}

	public DealerMessage(ControlPacketType controlPacketType, Payload payload) {
		this.controlPacketType = controlPacketType;
		this.topic = new Topic("");
		this.geofence = "";
		this.payload = payload;
	}

	public DealerMessage(ControlPacketType controlPacketType, Topic topic, String geofence, Payload payload) {
		this.controlPacketType = controlPacketType;
		this.topic = topic;
		this.geofence = geofence;
		this.payload = payload;
	}

	public ZMsg getZmsg() {
		ZMsg msg = ZMsg.newStringMsg(controlPacketType.name(), topic.getTopic(), geofence);
		msg.add(JSONable.toJSON(payload).getBytes()); // cannot just add string, encoding fails
		return msg;
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

	public Payload getPayload() {
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
