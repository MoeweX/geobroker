package de.hasenburg.geobroker.main;

import de.hasenburg.geobroker.communication.ControlPacketType;
import de.hasenburg.geobroker.communication.ZMQProcessManager;
import de.hasenburg.geobroker.model.InternalClientMessage;
import de.hasenburg.geobroker.model.Topic;
import de.hasenburg.geobroker.model.clients.ClientDirectory;
import de.hasenburg.geobroker.model.payload.CONNECTPayload;
import de.hasenburg.geobroker.model.payload.PINGREQPayload;
import de.hasenburg.geobroker.model.payload.PUBLISHPayload;
import de.hasenburg.geobroker.model.payload.SUBSCRIBEPayload;
import de.hasenburg.geobroker.model.spatial.Geofence;
import de.hasenburg.geobroker.model.spatial.Location;
import de.hasenburg.geobroker.model.storage.TopicAndGeofenceMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LoadTest {

	private static final Logger logger = LogManager.getLogger();

	ZMQProcessManager processManager;
	ClientDirectory clientDirectory;

	public static void main (String[] args) throws Exception {
	    LoadTest loadTest = new LoadTest();
	    loadTest.setUp();
	    loadTest.loadTestOwnTopic();
	    loadTest.tearDown();
	}

	public void setUp() {
		logger.info("Running setUp");
		BenchmarkHelper.startBenchmarking();
		ClientDirectory clientDirectory = new ClientDirectory();
		TopicAndGeofenceMapper topicAndGeofenceMapper = new TopicAndGeofenceMapper(new Configuration());

		processManager = new ZMQProcessManager();
		processManager.runZMQProcess_Broker("tcp://localhost", 5559, "broker");
		processManager.runZMQProcess_MessageProcessor("message_processor1", clientDirectory, topicAndGeofenceMapper);
		processManager.runZMQProcess_MessageProcessor("message_processor2", clientDirectory, topicAndGeofenceMapper);
		//processManager.runZMQProcess_MessageProcessor("message_processor3", clientDirectory);
	}

	public void tearDown() {
		logger.info("Running tearDown.");
		processManager.tearDown(5000);
		BenchmarkHelper.stopBenchmarking();
		System.exit(0);
	}

	public void loadTestOwnTopic() throws InterruptedException {
		logger.info("RUNNING testOneLocations");

		List<Thread> clients = new ArrayList<>();
		int numberOfClients = 5;

		Location location = Location.random();
		Geofence geofence = Geofence.circle(location, 0.0); // does not matter as topics are different

		// create clients
		for (int i = 0; i < numberOfClients; i++) {
			Thread client = new Thread(new SubscribeOwnTopicProcess("tcp://localhost", 5559, 200));
			clients.add(client);
		}

		Utility.sleepNoLog(3000, 0);
		for (Thread client : clients) {
			client.start();
		}

		// wait for clients
		for (Thread client : clients) {
			client.join();
		}

		// wait for receiving to stop
		logger.info("FINISHED");
	}

	class SubscribeOwnTopicProcess implements Runnable {

		SimpleClient simpleClient;
		int plannedMessageRounds;
		int actualMessageRounds = 1;
		Location location = Location.random();
		HashMap<ControlPacketType, Integer> numbers = new HashMap<>();

		public SubscribeOwnTopicProcess(String address, int port, int messagesToSend) {
			this.simpleClient = new SimpleClient(null, address, port, processManager);
			this.plannedMessageRounds = messagesToSend;

			simpleClient.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.CONNECT, new CONNECTPayload(location)));
			logger.info(simpleClient.receiveInternalClientMessage());
			simpleClient.sendInternalClientMessage(new InternalClientMessage(ControlPacketType.SUBSCRIBE,
					new SUBSCRIBEPayload(new Topic(simpleClient.getIdentity()), Geofence.circle(location, 0.0))));
			logger.info(simpleClient.receiveInternalClientMessage());
		}

		private void sendMessageAndProcessResponses(InternalClientMessage message, int expectedResponses) {
			simpleClient.sendInternalClientMessage(message);
			for (int i = 0; i < expectedResponses; i++) {
				InternalClientMessage response = simpleClient.receiveInternalClientMessage();
				logger.trace(response);
				if (message == null) {
					throw new RuntimeException("Broker answers with invalid messages!!");
				}
				Integer amount = numbers.get(message.getControlPacketType());
				if (amount == null) {
					amount = 0;
				}
				amount++;
				numbers.put(message.getControlPacketType(), amount);
			}
		}

		@Override
		public void run() {
			long start = System.currentTimeMillis();

			while (actualMessageRounds <= plannedMessageRounds) {

				double percentComplete = (double) actualMessageRounds/ plannedMessageRounds * 100;
				if (percentComplete % 5 == 0) {
					logger.info("Finished {}% of all planned message rounds", percentComplete);
				}

				long time = System.nanoTime();
				sendMessageAndProcessResponses(new InternalClientMessage(ControlPacketType.PINGREQ, new PINGREQPayload(location)), 1);
				BenchmarkHelper.addEntry("clientPINGREQ", System.nanoTime() - time);
				time = System.nanoTime();
				sendMessageAndProcessResponses(new InternalClientMessage(ControlPacketType.PUBLISH,
						new PUBLISHPayload(
								new Topic(simpleClient.getIdentity()),
								Geofence.circle(location, 0.0),
								"Some Test content that is being published.")),
						2);
				BenchmarkHelper.addEntry("clientPUBLISH", System.nanoTime() - time);

				actualMessageRounds++;
			}

			logger.info("Client {} finished {} message rounds in {} milliseconds",
					simpleClient.getIdentity(), plannedMessageRounds, System.currentTimeMillis() - start);

			logger.info("Results {}: {}", simpleClient.getIdentity(), numbers);
			simpleClient.tearDownClient();
		}

	}

}
