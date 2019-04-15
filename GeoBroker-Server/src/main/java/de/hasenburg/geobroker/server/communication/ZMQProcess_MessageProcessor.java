package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcess;
import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.payloads.*;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.matching.IMatchingLogic;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class ZMQProcess_MessageProcessor extends ZMQProcess {

	private static final Logger logger = LogManager.getLogger();

	private final IMatchingLogic matchingLogic;

	private int numberOfProcessedMessages = 0;

	// socket index
	private final int PROCESSOR_INDEX = 0;
	// TODO Add broker communicator push socket

	ZMQProcess_MessageProcessor(String identity, IMatchingLogic matchingLogic) {
		super(identity);
		this.matchingLogic = matchingLogic;
	}

	@Override
	protected List<Socket> bindAndConnectSockets(ZContext context) {
		Socket[] socketArray = new Socket[1];

		Socket processor = context.createSocket(SocketType.DEALER);
		processor.setIdentity(identity.getBytes());
		processor.connect(ZMQProcess_Server.SERVER_INPROC_ADDRESS);
		socketArray[PROCESSOR_INDEX] = processor;

		return Arrays.asList(socketArray);
	}

	@Override
	protected void processZMQControlCommandOtherThanKill(ZMQControlUtility.ZMQControlCommand zmqControlCommand, ZMsg msg) {
		// no other commands are of interest
	}

	@Override
	protected void processZMsg(int socketIndex, ZMsg msg) {

		if (socketIndex != PROCESSOR_INDEX) {
			logger.error("Cannot process message for socket at index {}, as this index is not known.", socketIndex);
		}

		// start processing the message
		numberOfProcessedMessages++;

		Optional<InternalServerMessage> messageO = InternalServerMessage.buildMessage(msg);
		logger.trace("ZMQProcess_MessageProcessor {} processing message number {}", identity, numberOfProcessedMessages);

		if (messageO.isPresent()) {
			InternalServerMessage message = messageO.get();
			switch (message.getControlPacketType()) {
				case CONNECT:
					matchingLogic.processCONNECT(message, sockets.get(PROCESSOR_INDEX), null);
					break;
				case DISCONNECT:
					matchingLogic.processDISCONNECT(message, sockets.get(PROCESSOR_INDEX), null);
					break;
				case PINGREQ:
					matchingLogic.processPINGREQ(message, sockets.get(PROCESSOR_INDEX), null);
					break;
				case SUBSCRIBE:
					matchingLogic.processSUBSCRIBE(message, sockets.get(PROCESSOR_INDEX), null);
					break;
				case UNSUBSCRIBE:
					matchingLogic.processUNSUBSCRIBE(message, sockets.get(PROCESSOR_INDEX), null);
					break;
				case PUBLISH:
					matchingLogic.processPUBLISH(message, sockets.get(PROCESSOR_INDEX), null);
					break;
				case BrokerForwardPublish:
					matchingLogic.processBrokerForwardPublish(message, sockets.get(PROCESSOR_INDEX), null);
					break;
				default:
					logger.warn("Cannot process message {}", message.toString());
			}
		} else {
			logger.warn("Received an incompatible message: {}", msg);
		}

	}

	@Override
	protected void shutdownCompleted() {
		logger.info("Shut down ZMQProcess_MessageProcessor {}", identity);
	}

}
