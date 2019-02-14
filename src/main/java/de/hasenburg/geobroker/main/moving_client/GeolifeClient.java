package de.hasenburg.geobroker.main.moving_client;

import de.hasenburg.geobroker.communication.ControlPacketType;
import de.hasenburg.geobroker.communication.ReasonCode;
import de.hasenburg.geobroker.communication.ZMQProcessManager;
import de.hasenburg.geobroker.main.BenchmarkClient;
import de.hasenburg.geobroker.main.Utility;
import de.hasenburg.geobroker.model.InternalClientMessage;
import de.hasenburg.geobroker.model.Topic;
import de.hasenburg.geobroker.model.payload.*;
import de.hasenburg.geobroker.model.spatial.Geofence;
import de.hasenburg.geobroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GeolifeClient extends BenchmarkClient {

	private static final Logger logger = LogManager.getLogger();
	private Configuration c;
	private Route route;
	private Thread t;

	public GeolifeClient(Configuration c, int index, Route r, ZMQProcessManager processManager) {
		super(c.getManagerName() + "-" + index, c.getAddress(), c.getPort(), processManager);
		this.route = r;
		this.c = c;
	}

	public void start() {
		logger.debug("Started geolife client {}", getIdentity());
		InternalClientMessage clientMessage = new InternalClientMessage(ControlPacketType.CONNECT,
																		new CONNECTPayload(route
																								   .getVisitedLocations()
																								   .get(0).left));
		sendInternalClientMessage(clientMessage);
		t = new Thread(() -> {
			Topic topic = new Topic("data");
			String data = Utility.generatePayloadWithSize(c.getPayloadSize(), getIdentity());

			while (!t.isInterrupted()) {
				logger.info("(Re-)starting route");
				for (ImmutablePair<Location, Integer> visitedLocation : route.getVisitedLocations()) {
					logger.debug("Driving to location {} in {} seconds", visitedLocation.left, visitedLocation.right);
					try {
						Thread.sleep(visitedLocation.right * 1000);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
					logger.trace("Arrived at location, transmitting data");
					Geofence fence = Geofence.circle(visitedLocation.left, c.getGeofenceSize());
					InternalClientMessage pingreq = new InternalClientMessage(ControlPacketType.PINGREQ,
																			  new PINGREQPayload(visitedLocation.left));
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// finished
						Thread.currentThread().interrupt();
						break;
					}
					sendInternalClientMessage(pingreq);
					InternalClientMessage subscribe = new InternalClientMessage(ControlPacketType.SUBSCRIBE,
																				new SUBSCRIBEPayload(topic, fence));
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// finished
						Thread.currentThread().interrupt();
						break;
					}
					sendInternalClientMessage(subscribe);
					InternalClientMessage publish = new InternalClientMessage(ControlPacketType.PUBLISH,
																			  new PUBLISHPayload(topic, fence, data));
					sendInternalClientMessage(publish);
				}
			}
		}); t.setName(getIdentity());
		t.start();
	}

	public void stop() {
		logger.info("Stopping GeolifeClient {}", getIdentity());
		t.interrupt();
		InternalClientMessage clientMessage = new InternalClientMessage(ControlPacketType.DISCONNECT,
																		new DISCONNECTPayload(ReasonCode.NormalDisconnection));
		sendInternalClientMessage(clientMessage);
		tearDownClient();
	}

	public static void main (String[] args) {
		Configuration con = new Configuration();
		ZMQProcessManager processManager = new ZMQProcessManager();
	    for (int i = 0; i < 500; i++) {
	    	GeolifeClient c = new GeolifeClient(con, i, null, processManager);
		}
		Utility.sleepNoLog(2000, 0);
	    logger.info("Could initiate clients");
	}

}
