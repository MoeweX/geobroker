package de.hasenburg.geofencebroker.main.moving_client;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.main.StorageClient;
import de.hasenburg.geofencebroker.model.InternalClientMessage;
import de.hasenburg.geofencebroker.model.payload.CONNECTPayload;
import de.hasenburg.geofencebroker.model.payload.DISCONNECTPayload;
import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class GeolifeClient extends StorageClient {

	private static final Logger logger = LogManager.getLogger();
	private Route route;
	Thread t;

	public GeolifeClient(Configuration c, int index, Route r, ZMQProcessManager processManager) throws IOException {
		super(c.getManagerName()+"-"+index, c.getAddress(), c.getPort(), processManager);
		this.route = r;
	}

	public void start() {
		logger.debug("Started geolife client {}", getIdentity());
		InternalClientMessage
				clientMessage = new InternalClientMessage(ControlPacketType.CONNECT, new CONNECTPayload(route.getVisitedLocations().get(0).left));
		sendInternalClientMessage(clientMessage);
		t = new Thread(() -> {
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
						// TODO subscribe to area around myself
						// TODO update current location
						// TODO publish data
					}
				}
			}
		);
		t.setName(getIdentity());
		t.start();
	}

	public void stop() {
		logger.info("Stopping GeolifeClient {}", getIdentity());
		t.interrupt();
		InternalClientMessage
				clientMessage = new InternalClientMessage(ControlPacketType.DISCONNECT, new DISCONNECTPayload(ReasonCode.NormalDisconnection));
		sendInternalClientMessage(clientMessage);
		tearDownClient();
	}

}
