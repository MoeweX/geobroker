package de.hasenburg.geobroker.server.matching;

import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.payloads.*;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.communication.InternalServerMessage;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;

import java.util.Set;

/**
 * One GeoBroker instance that does not communicate with others. Uses the {@link TopicAndGeofenceMapper}.
 */
@SuppressWarnings("OptionalGetWithoutIsPresent") // see IMatchingLogic for reasoning
public class DisGBAtSubscriberMatchingLogic implements IMatchingLogic {

	private static final Logger logger = LogManager.getLogger();

	private final ClientDirectory clientDirectory;
	private final TopicAndGeofenceMapper topicAndGeofenceMapper;
	private final BrokerAreaManager brokerAreaManager;

	public DisGBAtSubscriberMatchingLogic(ClientDirectory clientDirectory, TopicAndGeofenceMapper topicAndGeofenceMapper,
										  BrokerAreaManager brokerAreaManager) {
		this.clientDirectory = clientDirectory;
		this.topicAndGeofenceMapper = topicAndGeofenceMapper;
		this.brokerAreaManager = brokerAreaManager;
	}

	@Override
	public void processCONNECT(InternalServerMessage message, Socket clients, Socket brokers) {
		InternalServerMessage response;
		CONNECTPayload payload = message.getPayload().getCONNECTPayload().get();

		if (!handleResponsibility(message.getClientIdentifier(), payload.getLocation(), clients)) {
			return; // we are not responsible, client has been notified
		}

		boolean success = clientDirectory.addClient(message.getClientIdentifier(), payload.getLocation());

		if (success) {
			logger.debug("Created client {}, acknowledging.", message.getClientIdentifier());
			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.CONNACK,
												 new CONNACKPayload(ReasonCode.Success));
		} else {
			logger.debug("Client {} already exists, so protocol error. Disconnecting.", message.getClientIdentifier());
			clientDirectory.removeClient(message.getClientIdentifier());
			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.DISCONNECT,
												 new DISCONNECTPayload(ReasonCode.ProtocolError));
		}

		logger.trace("Sending response " + response);
		response.getZMsg().send(clients);
	}

	@Override
	public void processDISCONNECT(InternalServerMessage message, Socket clients, Socket brokers) {
		DISCONNECTPayload payload = message.getPayload().getDISCONNECTPayload().get();

		boolean success = clientDirectory.removeClient(message.getClientIdentifier());
		if (!success) {
			logger.trace("Client for {} did not exist", message.getClientIdentifier());
			return;
		}

		logger.debug("Disconnected client {}, code {}", message.getClientIdentifier(), payload.getReasonCode());

	}

	@Override
	public void processPINGREQ(InternalServerMessage message, Socket clients, Socket brokers) {
		InternalServerMessage response;
		PINGREQPayload payload = message.getPayload().getPINGREQPayload().get();

		// check whether client has moved to another broker area
		if (!handleResponsibility(message.getClientIdentifier(), payload.getLocation(), clients)) {
			// TODO F: migrate client data to other broker, right now he has to update the information himself
			clientDirectory.removeClient(message.getClientIdentifier());
			return; // we are not responsible, client has been notified
		}

		boolean success = clientDirectory.updateClientLocation(message.getClientIdentifier(), payload.getLocation());
		if (success) {
			logger.debug("Updated location of {} to {}", message.getClientIdentifier(), payload.getLocation());

			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.PINGRESP,
												 new PINGRESPPayload(ReasonCode.LocationUpdated));
		} else {
			logger.debug("Client {} is not connected", message.getClientIdentifier());
			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.PINGRESP,
												 new PINGRESPPayload(ReasonCode.NotConnected));
		}

		logger.trace("Sending response " + response);
		response.getZMsg().send(clients);
	}

	@Override
	public void processSUBSCRIBE(InternalServerMessage message, Socket clients, Socket brokers) {
		// TODO Implement
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public void processUNSUBSCRIBE(InternalServerMessage message, Socket clients, Socket brokers) {
		// TODO Implement
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public void processPUBLISH(InternalServerMessage message, Socket clients, Socket brokers) {
		// TODO Implement
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public void processBrokerForwardPublish(InternalServerMessage message, Socket clients, Socket brokers) {
		// TODO Implement
		throw new RuntimeException("Not yet implemented");
	}

	/*****************************************************************
	 * Message Processing Helper
	 ****************************************************************/

	/**
	 * Checks whether this particular broker is responsible for the client with the given location.
	 * If not, sends a disconnect message with the responsible broker, if any exists.
	 * Otherwise, does nothing
	 *
	 * @return true, if this broker is responsible, otherwise false
	 */
	private boolean handleResponsibility(String clientIdentifier, Location clientLocation, Socket clients) {
		if (!brokerAreaManager.checkIfResponsibleForClientLocation(clientLocation)) {
			// get responsible broker
			BrokerInfo repBroker = brokerAreaManager.getOtherBrokerForClientLocation(clientLocation);

			InternalServerMessage response = new InternalServerMessage(clientIdentifier,
																	   ControlPacketType.DISCONNECT,
																	   new DISCONNECTPayload(ReasonCode.WrongBroker, repBroker));
			logger.debug("Not responsible for client {}, responsible broker is {}", clientIdentifier, repBroker);

			response.getZMsg().send(clients);
			return false;
		}
		return true;
	}
}
