package de.hasenburg.geofencebroker.model;

import de.hasenburg.geofencebroker.communication.ReasonCode;

import java.util.Objects;
import java.util.Optional;

public class PayloadDISCONNECT extends Payload {

	private Location location;

	public PayloadDISCONNECT() {

	}

	public PayloadDISCONNECT(ReasonCode reasonCode) {
		super(reasonCode);
	}

	/*****************************************************************
	 * Test Main
	 ****************************************************************/

	public static void main (String[] args) {
		System.out.println(JSONable.toJSON(new PayloadDISCONNECT(ReasonCode.NormalDisconnection)));
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof PayloadDISCONNECT)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		PayloadDISCONNECT that = (PayloadDISCONNECT) o;
		return Objects.equals(location, that.location);
	}

	@Override
	public int hashCode() {

		return Objects.hash(super.hashCode(), location);
	}
}
