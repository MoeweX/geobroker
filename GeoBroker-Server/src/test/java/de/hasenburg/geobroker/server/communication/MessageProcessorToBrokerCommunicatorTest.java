package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNACKPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNECTPayload;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.distribution.IDistributionLogic;
import de.hasenburg.geobroker.server.matching.IMatchingLogic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

@SuppressWarnings({"OptionalGetWithoutIsPresent"})
public class MessageProcessorToBrokerCommunicatorTest {

	private static final Logger logger = LogManager.getLogger();

	@Test
	public void test() {
		String ourBrokerId = "ourBroker";

		ZMQProcessManager processManager = new ZMQProcessManager();

		// as TestDistributionLogic does not really send, does not matter what is put here as long as socket can be created
		BrokerInfo brokerInfo = new BrokerInfo("targetBroker", "localhost", 5559);

		ZMQProcess_BrokerCommunicator brokerCommunicator = ZMQProcessStarter.runZMQProcess_BrokerCommunicator(
				processManager,
				ourBrokerId,
				1,
				new TestDistributionLogic(),
				Collections.singletonList(brokerInfo));

		ZMQProcess_MessageProcessor messageProcessor = ZMQProcessStarter.runZMQProcess_MessageProcessor(processManager,
				ourBrokerId,
				1,
				new TestMatchingLogic(),
				1);


		// create a socket that sends to the message processor, the message will simply be forwarded to the broker communicator
		Socket req = processManager.getContext().createSocket(SocketType.DEALER);
		req.bind("inproc://" + ZMQProcess_Server.getServerIdentity(ourBrokerId)); // as this is where the message processor connects to

		new InternalServerMessage("clientOrigin",
				ControlPacketType.CONNECT,
				new CONNECTPayload(Location.random())).getZMsg().send(req);

		// wait for response
		ZMsg msg = ZMsg.recvMsg(req);
		InternalServerMessage response = InternalServerMessage.buildMessage(msg).get();
		logger.info("Received response" + response);
		assertEquals(ReasonCode.Success, response.getPayload().getCONNACKPayload().get().getReasonCode());

		assertEquals(1, messageProcessor.getNumberOfProcessedMessages());
		assertEquals(1, brokerCommunicator.getNumberOfProcessedMessages());
	}

	class TestMatchingLogic implements IMatchingLogic {

		@Override
		public void processCONNECT(InternalServerMessage message, Socket clients, Socket brokers) {
			logger.info("Received message " + message);

			assertEquals(ControlPacketType.CONNECT, message.getControlPacketType());

			// just send whatever message
			InternalBrokerMessage ibm = new InternalBrokerMessage(ControlPacketType.CONNECT,
					new CONNECTPayload(Location.random()));
			ZMsg msg = ZMQProcess_BrokerCommunicator.generatePULLSocketMessage("targetBroker", ibm);
			logger.info("Sending message {} to broker communicator", msg);
			msg.send(brokers);

			// respond
			msg = new InternalServerMessage(message.getClientIdentifier(),
					ControlPacketType.CONNACK,
					new CONNACKPayload(ReasonCode.Success)).getZMsg();
			logger.info("Sent message back to main test routine {}", msg);
			msg.send(clients);
		}

		@Override
		public void processDISCONNECT(InternalServerMessage message, Socket clients, Socket brokers) {
			processCONNECT(message, clients, brokers);
		}

		@Override
		public void processPINGREQ(InternalServerMessage message, Socket clients, Socket brokers) {
			processCONNECT(message, clients, brokers);
		}

		@Override
		public void processSUBSCRIBE(InternalServerMessage message, Socket clients, Socket brokers) {
			processCONNECT(message, clients, brokers);
		}

		@Override
		public void processUNSUBSCRIBE(InternalServerMessage message, Socket clients, Socket brokers) {
			processCONNECT(message, clients, brokers);
		}

		@Override
		public void processPUBLISH(InternalServerMessage message, Socket clients, Socket brokers) {
			processCONNECT(message, clients, brokers);
		}

		@Override
		public void processBrokerForwardPublish(InternalServerMessage message, Socket clients, Socket brokers) {
			processCONNECT(message, clients, brokers);
		}
	}

	class TestDistributionLogic implements IDistributionLogic {

		@Override
		public void sendMessageToOtherBrokers(ZMsg msg, Socket broker, String targetBrokerId) {
			logger.info("Received message " + msg);
		}

		@Override
		public void processOtherBrokerAcknowledgement(ZMsg msg, String otherBrokerId) {
			logger.info("Received message " + msg);
		}
	}

}