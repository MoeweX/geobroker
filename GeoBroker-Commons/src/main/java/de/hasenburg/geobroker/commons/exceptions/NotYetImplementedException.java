package de.hasenburg.geobroker.commons.exceptions;

public class NotYetImplementedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public NotYetImplementedException() {
		super("The called method is not yet implemented.");
	}

	public NotYetImplementedException(String message) {
		super(message);
	}

	public NotYetImplementedException(String message, Throwable cause) {
		super(message, cause);
	}
}
