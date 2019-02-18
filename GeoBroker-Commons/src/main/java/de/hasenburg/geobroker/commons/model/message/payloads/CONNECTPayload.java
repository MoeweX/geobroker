package de.hasenburg.geobroker.commons.model.message.payloads;

import de.hasenburg.geobroker.commons.model.spatial.Location;

import java.util.Objects;

public class CONNECTPayload extends AbstractPayload {

	protected Location location;

	public CONNECTPayload() {

	}

	public CONNECTPayload(Location location) {
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
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CONNECTPayload that = (CONNECTPayload) o;
		return Objects.equals(getLocation(), that.getLocation());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getLocation());
	}
}

