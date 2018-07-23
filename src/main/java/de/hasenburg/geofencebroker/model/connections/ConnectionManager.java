package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.communication.RouterCommunicator;
import de.hasenburg.geofencebroker.model.Payload;
import de.hasenburg.geofencebroker.model.PayloadPINGREQ;
import de.hasenburg.geofencebroker.model.RouterMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectionManager {

	private static final Logger logger = LogManager.getLogger();

	private final HashMap<String, Connection> connections = new HashMap<>();
	private RouterCommunicator routerCommunicator;

	/*****************************************************************
	 * Constructor and Co
	 ****************************************************************/

	public ConnectionManager(RouterCommunicator router) {
		this.routerCommunicator = router;
	}

	public List<Connection> getActiveConnections() {
		return new ArrayList<>(connections.values());
	}

	/*****************************************************************
	 * Message Processing
	 ****************************************************************/

	public boolean notExpectedMessage(RouterMessage message, ControlPacketType expectedType) {
		if (message.getControlPacketType() != expectedType) {
			logger.warn("Stopping processing of message from client {} as it has not expected ControlPacketType {}, instead {}",
					message.getClientIdentifier(), expectedType, message.getControlPacketType());
			return true;
		}
		logger.trace("Processing {} message from client {}", expectedType, message.getClientIdentifier());
		return false;
	}

	public boolean topicSet(RouterMessage message) {
		if (message.getTopic() != null) {
			logger.trace("Message has a valid topic");
			return true;
		}
		logger.warn("Message misses a topic");
		return false;
	}

	public void processCONNECT(RouterMessage message) {
		if (notExpectedMessage(message, ControlPacketType.CONNECT)) {
			return;
		}

		RouterMessage response;

		// get connection or create new
		Connection connection = connections.get(message.getClientIdentifier());
		if (connection != null) {
			logger.debug("Connection already exists for {}, so protocol error. Disconnecting.", message.getClientIdentifier());
			connections.remove(message.getClientIdentifier());
			response = new RouterMessage(
					message.getClientIdentifier(), ControlPacketType.DISCONNECT,
					new Payload(ReasonCode.ProtocolError));
		} else {
			connection = new Connection(message.getClientIdentifier());
			logger.debug("Created connection for client {}, acknowledging.", message.getClientIdentifier());
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.CONNACK);
			connections.put(connection.getClientIdentifier(), connection);
		}

		routerCommunicator.sendRouterMessage(response);
	}

	public void processDISCONNECT(RouterMessage message) {
		if (notExpectedMessage(message, ControlPacketType.DISCONNECT)) {
			return;
		}

		// get connection
		Connection connection = connections.get(message.getClientIdentifier());
		if (connection == null) {
			logger.trace("Connection for {} did not exist", message.getClientIdentifier());
			return;
		}

		// remove connection
		logger.debug("Disconnected client {}", message.getClientIdentifier());
		connections.remove(connection.getClientIdentifier());
	}

	public void processPINGREQ(RouterMessage message) {
		if (notExpectedMessage(message, ControlPacketType.PINGREQ)) {
			return;
		}

		Connection connection = connections.get(message.getClientIdentifier());
		RouterMessage response;

		if (connection != null) {
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PINGRESP);

			// update location
			PayloadPINGREQ payloadPINGREQ = (PayloadPINGREQ) message.getPayload();
			payloadPINGREQ.getLocation().ifPresentOrElse(location -> {
				connection.updateLocation(location);
				logger.debug("Updated location of {} to {}", message.getClientIdentifier(), location.toString());
			}, () -> {
				connection.updateHeartbeat();
				logger.warn("Message from {} misses location", message.getClientIdentifier());
			});

			connections.put(connection.getClientIdentifier(), connection);
		} else {
			logger.trace("Client {} is not connected", message.getClientIdentifier());
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PINGRESP,
					new Payload(ReasonCode.NotConnected));
		}

		routerCommunicator.sendRouterMessage(response);

	}

	public void processSUBSCRIBEforConnection(RouterMessage message) {
		if (notExpectedMessage(message, ControlPacketType.SUBSCRIBE)) {
			return;
		}

		RouterMessage response;

		if (topicSet(message)) {
			Connection connection = connections.get(message.getClientIdentifier());

			if (connection != null) {
				Subscription subscription = new Subscription(message.getTopic(), message.getGeofence());
				connection.putSubscription(subscription);
				connections.put(connection.getClientIdentifier(), connection);
				logger.debug("Client {} subscribed to topic {}", message.getClientIdentifier(), message.getTopic());
				response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.SUBACK,
						new Payload(ReasonCode.GrantedQoS0));
			} else {
				response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.SUBACK,
						new Payload(ReasonCode.NotConnected));
			}

		} else {
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.SUBACK,
					new Payload(ReasonCode.ProtocolError));
		}

		routerCommunicator.sendRouterMessage(response);

	}

	public void processUNSUBSCRIBEforConnection(RouterMessage message) {
		throw new RuntimeException("Not yet implemented");
	}

	public void processPublish(RouterMessage message) {
		if (notExpectedMessage(message, ControlPacketType.PUBLISH)) {
			return;
		}

		if (topicSet(message)) {
			List<Connection> activeConnections = getActiveConnections();
			logger.debug("Publishing topic {} to all subscribers", message.getTopic());

			// send message to every connection that has topic
			for (Connection connection : activeConnections) {
				if (connection.subscribedToTopic(message.getTopic())) {
					RouterMessage toPublish = new RouterMessage(connection.getClientIdentifier(),
							ControlPacketType.PUBLISH, message.getTopic(), message.getGeofence(), message.getPayload());
					routerCommunicator.sendRouterMessage(toPublish);
				}
			}
		}

	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder("\n");
		for (Map.Entry<String, Connection> entry : connections.entrySet()) {
			s.append("\t").append(entry.getValue().toString());
		}
		return s.toString();
	}

}
