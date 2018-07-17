package de.hasenburg.geofencebroker.exceptions;

public class RouterException extends Exception {

	private static final long serialVersionUID = 1L;

	public RouterException(String message) {
		super(message);
	}

	public RouterException(String message, Throwable cause) {
		super(message, cause);
	}

}
