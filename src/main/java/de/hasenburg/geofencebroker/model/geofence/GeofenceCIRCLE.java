package de.hasenburg.geofencebroker.model.geofence;

import de.hasenburg.geofencebroker.model.JSONable;
import de.hasenburg.geofencebroker.model.Location;

import java.util.Objects;

public class GeofenceCIRCLE implements JSONable {

	private Location circleLocation;
	private double circleDiameterInMeter;

	private GeofenceCIRCLE() {
		// JSONAble constructor
	}

	public GeofenceCIRCLE(Location location, double diameterInMeter) {
		this.circleLocation = location;
		this.circleDiameterInMeter = diameterInMeter;
	}

	public boolean locationInFence(Location location) {
		return Location.distanceInMeters(circleLocation, location) <= circleDiameterInMeter;
	}

	public Location getCircleLocation() {
		return circleLocation;
	}

	public double getCircleDiameterInMeter() {
		return circleDiameterInMeter;
	}

	// TODO Add 10% of diameter as tolerance to equals
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof GeofenceCIRCLE)) {
			return false;
		}
		GeofenceCIRCLE that = (GeofenceCIRCLE) o;
		return Double.compare(that.getCircleDiameterInMeter(), getCircleDiameterInMeter()) == 0 &&
				Objects.equals(getCircleLocation(), that.getCircleLocation());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getCircleLocation(), getCircleDiameterInMeter());
	}
}
