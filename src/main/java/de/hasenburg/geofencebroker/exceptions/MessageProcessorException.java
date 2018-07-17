package de.hasenburg.geofencebroker.exceptions;

public class MessageProcessorException extends Exception {

	private static final long serialVersionUID = 1L;

	public MessageProcessorException(String message) {
		super(message);
	}

	public MessageProcessorException(String message, Throwable cause) {
		super(message, cause);
	}

}
