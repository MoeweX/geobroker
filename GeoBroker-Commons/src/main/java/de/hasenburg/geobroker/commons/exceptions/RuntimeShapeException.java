package de.hasenburg.geobroker.commons.exceptions;

public class RuntimeShapeException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RuntimeShapeException(String message) {
		super(message);
	}

	public RuntimeShapeException(String message, Throwable cause) {
		super(message, cause);
	}

}
