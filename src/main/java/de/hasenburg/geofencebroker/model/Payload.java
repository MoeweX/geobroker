package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ReasonCode;

import java.util.Objects;
import java.util.Optional;

/**
 * Every {@link DealerMessage} or {@link RouterMessage} has at least this payload, with no fields.
 *
 * All fields of this class and the fields of subclasses are optional.
 */
public class Payload implements JSONable {

	private ReasonCode reasonCode;

	public Payload() {

	}

	public Payload(ReasonCode reasonCode) {
		this.reasonCode = reasonCode;
	}

	public Optional<ReasonCode> getReasonCode() {
		return Optional.ofNullable(reasonCode);
	}

	/*****************************************************************
	 * Test Main
	 ****************************************************************/

	public static void main (String[] args) {
		System.out.println(JSONable.toJSON(new Payload()));
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	public void setReasonCode(ReasonCode reasonCode) {
		this.reasonCode = reasonCode;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Payload)) {
			return false;
		}
		Payload payload = (Payload) o;
		return getReasonCode() == payload.getReasonCode();
	}

	@Override
	public int hashCode() {

		return Objects.hash(getReasonCode());
	}

	@Override
	public String toString() {
		return JSONable.toJSON(this);
	}
}
