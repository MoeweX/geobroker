package de.hasenburg.geofencebroker.main.exceptions;

public class DealerException extends Exception {

	private static final long serialVersionUID = 1L;

	public DealerException(String message) {
		super(message);
	}

	public DealerException(String message, Throwable cause) {
		super(message, cause);
	}

}
