package de.hasenburg.geofencebroker.model.spatial;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hasenburg.geofencebroker.model.JSONable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.spatial4j.shape.Point;

import java.util.Objects;
import java.util.Optional;
import java.util.Random;

import static de.hasenburg.geofencebroker.model.spatial.SpatialContext.GEO;

// TODO: use wktString as JSON and String output
public class Location implements JSONable {

	private static final Logger logger = LogManager.getLogger();
	@JsonIgnore
	private final Point point;

	public Location(Point point) {
		this.point = point;
	}

	@JsonCreator
	public Location(@JsonProperty("lat") double lat, @JsonProperty("lon") double lon) {
		point = GEO.getShapeFactory().pointLatLon(lat, lon);
	}

	/**
	 * Creates a random location (Not inclusive of (-90, 0))
	 */
	public static Location random() {
		Random random = new Random();
		return new Location((random.nextDouble() * -180.0) + 90.0, (random.nextDouble() * -360.0) + 180.0);
	}

	/*****************************************************************
	 * String serialization
	 ****************************************************************/

	public static Optional<Location> fromString(String s) {
		try {
			String[] split = s.substring(1, s.length() - 1).split(",");
			return Optional.of(new Location(Double.parseDouble(split[0]), Double.parseDouble(split[1])));
		} catch (Exception e) {
			logger.warn("Could not build Location from String {}", s);
			return Optional.empty();
		}
	}

	@Override
	public String toString() {
		return String.format("(%s,%s)", point.getLat(), point.getLon());
	}

	/*****************************************************************
	 * Convenience methods (also used by Jackson)
	 ****************************************************************/

	public double getLat() {
		return point.getLat();
	}

	public double getLon() {
		return point.getLon();
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	public Point getPoint() {
		return point;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Location location = (Location) o;
		return Objects.equals(getPoint(), location.getPoint());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getPoint());
	}
}
