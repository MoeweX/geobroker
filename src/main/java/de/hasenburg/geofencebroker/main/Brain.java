package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.main.communication.Dealer;
import de.hasenburg.geofencebroker.main.exceptions.DealerException;

public class Brain {

	public static void main(String[] args) throws DealerException {

		Dealer dealer = new Dealer("tcp://*", 5559);
		dealer.startReceiving();

	}

}
