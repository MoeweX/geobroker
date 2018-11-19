package de.hasenburg.geofencebroker.model.payload;

import de.hasenburg.geofencebroker.model.spatial.Location;

import java.util.Objects;

public class PINGREQPayload extends AbstractPayload {

	protected Location location;

	public PINGREQPayload() {

	}

	public PINGREQPayload(Location location) {
		super();
		this.location = location;
	}

	/*****************************************************************
	 * Getter & setter
	 ****************************************************************/

	public Location getLocation() {
		return location;
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof PINGREQPayload)) {
			return false;
		}
		PINGREQPayload that = (PINGREQPayload) o;
		return Objects.equals(getLocation(), that.getLocation());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getLocation());
	}

}
