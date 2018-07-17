package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.Router;
import de.hasenburg.geofencebroker.exceptions.RouterException;

public class Brain {

	public static void main(String[] args) throws RouterException {

		Router dealer = new Router("tcp://*", 5559);
		dealer.startReceiving();

	}

}
