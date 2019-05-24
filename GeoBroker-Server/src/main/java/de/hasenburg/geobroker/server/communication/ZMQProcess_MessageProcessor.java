package de.hasenburg.geobroker.server.communication;

import com.esotericsoftware.kryo.Kryo;
import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcess;
import de.hasenburg.geobroker.commons.model.KryoSerializer;
import de.hasenburg.geobroker.server.matching.IMatchingLogic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.*;

class ZMQProcess_MessageProcessor extends ZMQProcess {

	private static final Logger logger = LogManager.getLogger();

	private final String brokerId;
	private final int number;
	private final IMatchingLogic matchingLogic;
	private final int numberOfBrokerCommunicators;

	private int numberOfProcessedMessages = 0;
	public KryoSerializer kryo = new KryoSerializer();

	// socket index
	private final int PROCESSOR_INDEX = 0;
	private final int BROKER_COMMUNICATOR_INDEX = 1;

	/**
	 * @param brokerId - identity should be the broker id this message processor is running on
	 * @param number - incrementing number for this message processor (as there might be many), starts with 1
	 * @param numberOfBrokerCommunicators - how many bc exist, can be 0
	 */
	ZMQProcess_MessageProcessor(String brokerId, int number, IMatchingLogic matchingLogic,
								int numberOfBrokerCommunicators) {
		super(getMessageProcessorIdentity(brokerId, number));
		this.brokerId = brokerId;
		this.number = number;
		this.matchingLogic = matchingLogic;
		this.numberOfBrokerCommunicators = numberOfBrokerCommunicators;
	}

	static String getMessageProcessorIdentity(String brokerId, int number) {
		return brokerId + "-message_processor-" + number;
	}

	@Override
	protected List<Socket> bindAndConnectSockets(ZContext context) {
		Socket[] socketArray = new Socket[2];

		Socket processor = context.createSocket(SocketType.DEALER);
		processor.setIdentity(identity.getBytes());
		processor.connect("inproc://" + ZMQProcess_Server.getServerIdentity(brokerId));
		socketArray[PROCESSOR_INDEX] = processor;

		Socket bc = context.createSocket(SocketType.PUSH);
		// ok because processor and bc do not send both to this socket
		bc.setIdentity(identity.getBytes());
		for (int i = 1; i <= numberOfBrokerCommunicators; i++) {
			String brokerCommunicatorIdentity = ZMQProcess_BrokerCommunicator.getBrokerCommunicatorId(brokerId, i);
			bc.connect("inproc://" + brokerCommunicatorIdentity);
		}
		socketArray[BROKER_COMMUNICATOR_INDEX] = bc;

		return Arrays.asList(socketArray);
	}

	@Override
	protected void processZMQControlCommandOtherThanKill(ZMQControlUtility.ZMQControlCommand zmqControlCommand,
														 ZMsg msg) {
		// no other commands are of interest
	}

	@Override
	protected void processZMsg(int socketIndex, ZMsg msg) {

		if (socketIndex != PROCESSOR_INDEX) {
			logger.error("Cannot process message for socket at index {}, as this index is not known.", socketIndex);
		}

		// start processing the message
		numberOfProcessedMessages++;

		Optional<InternalServerMessage> messageO = InternalServerMessage.buildMessage(msg, kryo);
		logger.trace("ZMQProcess_MessageProcessor {} processing message number {}",
				identity,
				numberOfProcessedMessages);

		if (messageO.isPresent()) {
			InternalServerMessage message = messageO.get();

			Socket clientsSocket = sockets.get(PROCESSOR_INDEX);
			Socket brokersSocket = sockets.get(BROKER_COMMUNICATOR_INDEX);
			switch (message.getControlPacketType()) {
				case CONNECT:
					matchingLogic.processCONNECT(message, clientsSocket, brokersSocket, kryo);
					break;
				case DISCONNECT:
					matchingLogic.processDISCONNECT(message, clientsSocket, brokersSocket, kryo);
					break;
				case PINGREQ:
					matchingLogic.processPINGREQ(message, clientsSocket, brokersSocket, kryo);
					break;
				case SUBSCRIBE:
					matchingLogic.processSUBSCRIBE(message, clientsSocket, brokersSocket, kryo);
					break;
				case UNSUBSCRIBE:
					matchingLogic.processUNSUBSCRIBE(message, clientsSocket, brokersSocket, kryo);
					break;
				case PUBLISH:
					matchingLogic.processPUBLISH(message, clientsSocket, brokersSocket, kryo);
					break;
				case BrokerForwardDisconnect:
					matchingLogic.processBrokerForwardDisconnect(message, clientsSocket, brokersSocket, kryo);
					break;
				case BrokerForwardPingreq:
					matchingLogic.processBrokerForwardPingreq(message, clientsSocket, brokersSocket, kryo);
					break;
				case BrokerForwardSubscribe:
					matchingLogic.processBrokerForwardSubscribe(message, clientsSocket, brokersSocket, kryo);
					break;
				case BrokerForwardUnsubscribe:
					matchingLogic.processBrokerForwardUnsubscribe(message, clientsSocket, brokersSocket, kryo);
					break;
				case BrokerForwardPublish:
					matchingLogic.processBrokerForwardPublish(message, clientsSocket, brokersSocket, kryo);
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
		logger.info("Shut down ZMQProcess_MessageProcessor {}", getMessageProcessorIdentity(identity, number));
	}

	public KryoSerializer getKryo() {
		return kryo;
	}

	int getNumberOfProcessedMessages() {
		return numberOfProcessedMessages;
	}
}
