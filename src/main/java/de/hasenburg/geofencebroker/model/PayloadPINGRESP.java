package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ReasonCode;

import java.util.Objects;

public class PayloadPINGRESP extends Payload {

	private Location location;

	public PayloadPINGRESP() {

	}

	public PayloadPINGRESP(ReasonCode reasonCode) {
		super(reasonCode);
	}

	/*****************************************************************
	 * Test Main
	 ****************************************************************/

	public static void main (String[] args) {
		System.out.println(JSONable.toJSON(new PayloadPINGRESP(ReasonCode.NotConnected)));
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof PayloadPINGRESP)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		PayloadPINGRESP that = (PayloadPINGRESP) o;
		return Objects.equals(location, that.location);
	}

	@Override
	public int hashCode() {

		return Objects.hash(super.hashCode(), location);
	}
}
