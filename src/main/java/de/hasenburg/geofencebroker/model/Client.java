package de.hasenburg.geofencebroker.model;

public class Client {

	private final String clientId; // every clientId may only exist once

	public Client(String clientId) {
		this.clientId = clientId;
	}
}
