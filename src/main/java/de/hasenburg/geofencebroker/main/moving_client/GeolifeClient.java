package de.hasenburg.geofencebroker.main.moving_client;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.communication.ZMQProcessManager;
import de.hasenburg.geofencebroker.main.SimpleClient;
import de.hasenburg.geofencebroker.model.InternalClientMessage;
import de.hasenburg.geofencebroker.model.payload.CONNECTPayload;
import de.hasenburg.geofencebroker.model.payload.DISCONNECTPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GeolifeClient extends SimpleClient {

	private static final Logger logger = LogManager.getLogger();
	private Route route;

	public GeolifeClient(Configuration c, int index, Route r, ZMQProcessManager processManager) {
		super(c.getManagerName()+"-"+index, c.getAddress(), c.getPort(), processManager);
		this.route = r;
	}

	public void start() {
		// TODO add threading and make it use route
		logger.debug("Started geolife client {}", getIdentity());
		InternalClientMessage
				clientMessage = new InternalClientMessage(ControlPacketType.CONNECT, new CONNECTPayload(route.getVisitedLocations().get(0).left));
		sendInternalClientMessage(clientMessage);
	}

	public void stop() {
		InternalClientMessage
				clientMessage = new InternalClientMessage(ControlPacketType.DISCONNECT, new DISCONNECTPayload(ReasonCode.NormalDisconnection));
		sendInternalClientMessage(clientMessage);
		tearDownClient();
	}

}
