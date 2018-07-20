package de.hasenburg.geofencebroker.model;

import java.util.Objects;
import java.util.Optional;

public class PayloadPINGREQ extends Payload {

	private Location location;

	public PayloadPINGREQ() {

	}

	public PayloadPINGREQ(Location location) {
		this.location = location;
	}

	public Optional<Location> getLocation() {
		return Optional.ofNullable(location);
	}

	/*****************************************************************
	 * Test Main
	 ****************************************************************/

	public static void main (String[] args) {
		System.out.println(JSONable.toJSON(new PayloadPINGREQ(Location.random())));
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	public void setLocation(Location location) {
		this.location = location;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof PayloadPINGREQ)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		PayloadPINGREQ that = (PayloadPINGREQ) o;
		return Objects.equals(getLocation(), that.getLocation());
	}

	@Override
	public int hashCode() {

		return Objects.hash(super.hashCode(), getLocation());
	}
}
