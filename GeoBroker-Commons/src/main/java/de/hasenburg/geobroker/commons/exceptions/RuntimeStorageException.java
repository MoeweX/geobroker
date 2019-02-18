package de.hasenburg.geobroker.commons.exceptions;

public class RuntimeStorageException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RuntimeStorageException(String message) {
		super(message);
	}

	public RuntimeStorageException(String message, Throwable cause) {
		super(message, cause);
	}

}
