package de.hasenburg.geofencebroker.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.Random;

public class Location {

	private static final Logger logger = LogManager.getLogger();

	private Double latitude;
	private Double longitude;

	private Location() {
		// JSON
	}

	/**
	 * Creates a random location point. (Not inclusive of (-90, 0))
	 */
	public static Location random() {
		Random random = new Random();
		return new Location((random.nextDouble() * -180.0) + 90.0, (random.nextDouble() * -360.0) + 180.0);
	}

	public static Optional<Location> fromString(String s) {
		try {
			String[] split = s.substring(1, s.length() - 1).split(",");
			return Optional.of(new Location(Double.parseDouble(split[0]), Double.parseDouble(split[1])));
		} catch (Exception e) {
			logger.warn("Could not build Location from String {}", s);
			return Optional.empty();
		}
	}

	/**
	 * @param latitude  the latitude in degrees.
	 * @param longitude the longitude in degrees.
	 */
	public Location(double latitude, double longitude) {
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public static double distanceInMeters(Location location1, Location location2) {
		double lat1 = location1.latitude;
		double lng1 = location1.longitude;
		double lat2 = location2.latitude;
		double lng2 = location2.longitude;

		double earthRadius = 6371000; //meters
		double radLat = Math.toRadians(lat2 - lat1);
		double radLng = Math.toRadians(lng2 - lng1);
		double a = Math.sin(radLat / 2) * Math.sin(radLat / 2) + Math.cos(Math.toRadians(lat1))
				* Math.cos(Math.toRadians(lat2)) * Math.sin(radLng / 2) * Math.sin(radLng / 2);
		double b = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

		return earthRadius * b;
	}

	@Override
	public String toString() {
		return String.format("(%s,%s)", latitude, longitude);
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	public Double getLatitude() {
		return latitude;
	}

	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}

	public Double getLongitude() {
		return longitude;
	}

	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Location)) {
			return false;
		}
		Location location = (Location) o;
		return Objects.equals(latitude, location.latitude) &&
				Objects.equals(longitude, location.longitude);
	}

	@Override
	public int hashCode() {

		return Objects.hash(latitude, longitude);
	}
}
