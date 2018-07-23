package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.communication.RouterCommunicator;
import de.hasenburg.geofencebroker.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
		return connections.values().stream().filter(c -> c.isActive()).collect(Collectors.toList());
	}

	public List<Connection> getInactiveConnections() {
		return connections.values().stream().filter(c -> !c.isActive()).collect(Collectors.toList());
	}

	/*****************************************************************
	 * Message Processing
	 ****************************************************************/

	/**
	 * @return Optional<Connection>, is empty when invalid ControlPacketType
	 */
	public Optional<Connection> processCONNECT(RouterMessage message) {
		if (message.getControlPacketType() != ControlPacketType.CONNECT) {
			return Optional.empty();
		}

		// get connection or create new
		Connection connection = connections.get(message.getClientIdentifier());
		if (connection == null) {
			connection = new Connection(message.getClientIdentifier());
		}

		RouterMessage response;

		if (connection.isActive()) {
			logger.debug("Connection of {} was already active, so protocol error. Closing now.", connection.getClientIdentifier());
			connection.setActive(false);
			response = new RouterMessage(
					message.getClientIdentifier(), ControlPacketType.DISCONNECT,
					new PayloadDISCONNECT(ReasonCode.ProtocolError));
		} else {
			logger.debug("Connection of {} is now active, acknowledging.", connection.getClientIdentifier());
			connection.setActive(true);
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.CONNACK);
		}

		routerCommunicator.sendRouterMessage(response);

		connections.put(connection.getClientIdentifier(), connection);
		return Optional.of(connection);
	}

	/**
	 * @return Optional<Connection>, is empty when invalid ControlPacketType or connection for clientIdentifier does not exist
	 */
	public Optional<Connection> processDISCONNECT(RouterMessage message) {
		if (message.getControlPacketType() != ControlPacketType.DISCONNECT) {
			return Optional.empty();
		}

		// get connection
		Connection connection = connections.get(message.getClientIdentifier());
		if (connection == null) {
			logger.debug("Connection of {} did not exist", message.getClientIdentifier());
			return Optional.empty();
		}

		// set to inactive
		logger.debug("Connection of {} is now inactive", message.getClientIdentifier());
		connection.setActive(false);

		connections.put(connection.getClientIdentifier(), connection);
		return Optional.of(connection);
	}

	public void processPINGREQ(RouterMessage message) {
		if (message.getControlPacketType() != ControlPacketType.PINGREQ) {
			return;
		}

		Connection connection = connections.get(message.getClientIdentifier());
		RouterMessage response;

		// only if connection exists and is active
		if (connection != null && connection.isActive()) {
			logger.trace("Received PINGREQ from active client {}", connection.getClientIdentifier());
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PINGRESP);

			// update location
			PayloadPINGREQ payloadPINGREQ = (PayloadPINGREQ) message.getPayload();
			payloadPINGREQ.getLocation().ifPresentOrElse(location -> {
				connection.updateLocation(location);
				connections.put(connection.getClientIdentifier(), connection);
			}, () -> logger.warn("No location send with message from client {}", connection.getClientIdentifier()));

		} else {
			logger.trace("Received PINGREQ from not connected client {}", message.getClientIdentifier());
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PINGRESP,
					new PayloadPINGRESP(ReasonCode.NotConnected));
		}

		routerCommunicator.sendRouterMessage(response);

	}

	/**
	 * Processes {@link de.hasenburg.geofencebroker.communication.ControlPacketType#SUBSCRIBE} messages and updates connections accordingly.
	 */
	public Optional<Subscription> processSUBSCRIBEforConnection(RouterMessage message) {
		// TODO Implement
		return Optional.empty();
	}

	/**
	 * Processes {@link de.hasenburg.geofencebroker.communication.ControlPacketType#UNSUBSCRIBE} messages and updates connections accordingly.
	 */
	public Optional<Subscription> processUNSUBSCRIBEforConnection(RouterMessage message) {
		// TODO Implement
		return Optional.empty();
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder("\n");
		for (Map.Entry<String, Connection> entry : connections.entrySet()) {
			s.append("\t").append(entry.getKey()).append(", is active: ").append(entry.getValue().isActive());
			s.append("\t\tLocation: ").append(entry.getValue().getLocation().get().toString());
			s.append("\n");
		}
		return s.toString();
	}
}
