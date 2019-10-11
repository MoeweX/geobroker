package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.communication.ZMQProcessManager;
import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.commons.model.KryoSerializer;
import de.hasenburg.geobroker.commons.model.message.PayloadKt;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.distribution.IDistributionLogic;
import de.hasenburg.geobroker.server.matching.IMatchingLogic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.Collections;

import static de.hasenburg.geobroker.commons.model.message.Payload.*;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"ConstantConditions"})
public class MessageProcessorToBrokerCommunicatorTest {

	private static final Logger logger = LogManager.getLogger();
	public KryoSerializer kryo = new KryoSerializer();
	ZMQProcessManager processManager = new ZMQProcessManager();

	@After
	public void after() {
		processManager.tearDown(5000);
	}

	@Test
	public void test() {
		String ourBrokerId = "ourBroker";


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
		req.bind("inproc://" +
				ZMQProcess_Server.getServerIdentity(ourBrokerId)); // as this is where the message processor connects to

		PayloadKt.payloadToZMsg(new CONNECTPayload(Location.random()), kryo, "clientOrigin").send(req);

		// wait for response
		ZMsg msg = ZMsg.recvMsg(req);
		CONNACKPayload payload = (CONNACKPayload) PayloadKt.transformZMsgWithId(msg, kryo).component2();
		logger.info("Received response" + payload);
		assertEquals(ReasonCode.Success, payload.getReasonCode());

		assertEquals(1, messageProcessor.getNumberOfProcessedMessages());
		assertEquals(1, brokerCommunicator.getNumberOfProcessedMessages());
	}

	class TestMatchingLogic implements IMatchingLogic {

		@Override
		public void processCONNECT(@NotNull String clientIdentifier, @NotNull CONNECTPayload payload,
								   @NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
								   @NotNull KryoSerializer kryo) {
			logger.info("Received payload " + payload);

			// just send whatever message
			CONNECTPayload connectPayload = new CONNECTPayload(Location.random());
			ZMsg msg = ZMQProcess_BrokerCommunicator.generatePULLSocketMessage("targetBroker",
					PayloadKt.payloadToZMsg(connectPayload, kryo, null));
			logger.info("Sending message {} to broker communicator", msg);
			msg.send(brokers);

			// respond
			msg = PayloadKt.payloadToZMsg(new CONNACKPayload(ReasonCode.Success), kryo, clientIdentifier);

			logger.info("Sent message back to main test routine {}", msg);
			msg.send(clients);
		}

		@Override
		public void processDISCONNECT(@NotNull String clientIdentifier, @NotNull DISCONNECTPayload payload,
									  @NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
									  @NotNull KryoSerializer kryo) {
			logger.warn("Unsupported operation, message is discarded");
		}

		@Override
		public void processPINGREQ(@NotNull String clientIdentifier, @NotNull PINGREQPayload payload,
								   @NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
								   @NotNull KryoSerializer kryo) {
			logger.warn("Unsupported operation, message is discarded");
		}

		@Override
		public void processSUBSCRIBE(@NotNull String clientIdentifier, @NotNull SUBSCRIBEPayload payload,
									 @NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
									 @NotNull KryoSerializer kryo) {
			logger.warn("Unsupported operation, message is discarded");
		}

		@Override
		public void processUNSUBSCRIBE(@NotNull String clientIdentifier, @NotNull UNSUBSCRIBEPayload payload,
									   @NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
									   @NotNull KryoSerializer kryo) {
			logger.warn("Unsupported operation, message is discarded");
		}

		@Override
		public void processPUBLISH(@NotNull String clientIdentifier, @NotNull PUBLISHPayload payload,
								   @NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
								   @NotNull KryoSerializer kryo) {
			logger.warn("Unsupported operation, message is discarded");
		}

		@Override
		public void processBrokerForwardDisconnect(@NotNull String otherBrokerId,
												   @NotNull BrokerForwardDisconnectPayload payload,
												   @NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
												   @NotNull KryoSerializer kryo) {
			logger.warn("Unsupported operation, message is discarded");
		}

		@Override
		public void processBrokerForwardPingreq(@NotNull String otherBrokerId,
												@NotNull BrokerForwardPingreqPayload payload,
												@NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
												@NotNull KryoSerializer kryo) {
			logger.warn("Unsupported operation, message is discarded");
		}

		@Override
		public void processBrokerForwardSubscribe(@NotNull String otherBrokerId,
												  @NotNull BrokerForwardSubscribePayload payload,
												  @NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
												  @NotNull KryoSerializer kryo) {
			logger.warn("Unsupported operation, message is discarded");
		}

		@Override
		public void processBrokerForwardUnsubscribe(@NotNull String otherBrokerId,
													@NotNull BrokerForwardUnsubscribePayload payload,
													@NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
													@NotNull KryoSerializer kryo) {
			logger.warn("Unsupported operation, message is discarded");
		}

		@Override
		public void processBrokerForwardPublish(@NotNull String otherBrokerId,
												@NotNull BrokerForwardPublishPayload payload,
												@NotNull ZMQ.Socket clients, @NotNull ZMQ.Socket brokers,
												@NotNull KryoSerializer kryo) {
			logger.warn("Unsupported operation, message is discarded");
		}
	}

	class TestDistributionLogic implements IDistributionLogic {

		@Override
		public void sendMessageToOtherBrokers(ZMsg msg, Socket broker, String targetBrokerId, KryoSerializer kryo) {
			logger.info("Received message " + msg);
		}

		@Override
		public void processOtherBrokerAcknowledgement(ZMsg msg, String otherBrokerId, KryoSerializer kryo) {
			logger.info("Received message " + msg);
		}
	}

}