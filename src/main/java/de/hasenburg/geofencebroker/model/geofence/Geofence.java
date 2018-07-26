package de.hasenburg.geofencebroker.model.geofence;

import de.hasenburg.geofencebroker.model.JSONable;
import de.hasenburg.geofencebroker.model.Location;

import java.util.Objects;

public class Geofence implements JSONable {

	private Location circleLocation;
	private double circleDiameterInMeter; // negative = infinite

	private Geofence() {
		// JSON
	}

	public Geofence(Location location, double diameterInMeter) {
		this.circleLocation = location;
		this.circleDiameterInMeter = diameterInMeter;
	}

	public boolean locationInFence(Location location) {
		if (circleDiameterInMeter < 0) {
			return true;
		}
		return Location.distanceInMeters(circleLocation, location) <= circleDiameterInMeter;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	public Location getCircleLocation() {
		return circleLocation;
	}

	public double getCircleDiameterInMeter() {
		return circleDiameterInMeter;
	}

	/**
	 * Two Geofences are equal if their diameter is similar and their locations are in close proximity (<10% of diameter)
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Geofence)) {
			return false;
		}
		Geofence that = (Geofence) o;

		boolean closeProximity = false;
		if (Location.distanceInMeters(circleLocation, that.circleLocation) <= 0.1 * circleDiameterInMeter) {
			closeProximity = true;
		}

		return Double.compare(that.getCircleDiameterInMeter(), getCircleDiameterInMeter()) == 0 && closeProximity;
	}

	@Override
	public int hashCode() {

		return Objects.hash(getCircleLocation(), getCircleDiameterInMeter());
	}

	@Override
	public String toString() {
		return "Geofence{" +
				"circleLocation=" + circleLocation +
				", circleDiameterInMeter=" + circleDiameterInMeter +
				'}';
	}
}
