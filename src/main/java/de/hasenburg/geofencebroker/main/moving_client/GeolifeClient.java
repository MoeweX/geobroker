package de.hasenburg.geofencebroker.main.moving_client;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.main.BenchmarkClient;
import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.InternalClientMessage;
import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.payload.*;
import de.hasenburg.geofencebroker.model.spatial.Geofence;
import de.hasenburg.geofencebroker.model.spatial.Location;
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
					Utility.sleep(1000, 0);
					sendInternalClientMessage(pingreq);
					InternalClientMessage subscribe = new InternalClientMessage(ControlPacketType.SUBSCRIBE,
																				new SUBSCRIBEPayload(topic, fence));
					Utility.sleep(1000, 0);
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

}
