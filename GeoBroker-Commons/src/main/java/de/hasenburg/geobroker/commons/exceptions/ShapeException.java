package de.hasenburg.geobroker.commons.exceptions;

public class ShapeException extends Exception {

	private static final long serialVersionUID = 1L;

	public ShapeException(String message) {
		super(message);
	}

	public ShapeException(String message, Throwable cause) {
		super(message, cause);
	}

}
