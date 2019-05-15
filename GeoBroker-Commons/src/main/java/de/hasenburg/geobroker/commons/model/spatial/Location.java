package de.hasenburg.geobroker.commons.model.spatial;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hasenburg.geobroker.commons.model.JSONable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.shape.Point;

import java.text.ParseException;
import java.util.Objects;
import java.util.Random;

import static de.hasenburg.geobroker.commons.model.spatial.SpatialContext.GEO;
import static org.locationtech.spatial4j.distance.DistanceUtils.DEG_TO_KM;

public class Location implements JSONable {

	private static final Logger logger = LogManager.getLogger();

	private final Point point;

	private Location(Point point) {
		this.point = point;
	}

	@JsonCreator
	private Location(@JsonProperty("WKT") String wkt) throws ParseException {
		WKTReader reader = (WKTReader) GEO.getFormats().getWktReader();
		this.point = (Point) reader.parse(wkt);
	}

	/**
	 * Creates a location with the given lat/lon coordinates.
	 *
	 * @param lat - the latitude
	 * @param lon - the longitude
	 */
	public Location(double lat, double lon) {
		point = GEO.getShapeFactory().pointLatLon(lat, lon);
	}

	/**
	 * Creates a random location (Not inclusive of (-90, 0))
	 */
	public static Location random() {
		Random random = new Random();
		// there have been rounding errors
		return new Location(Math.min((random.nextDouble() * -180.0) + 90.0, 90),
				Math.min((random.nextDouble() * -360.0) + 180.0, 180.0));
	}

	/**
	 * Distance between this location and the given one, as determined by the Haversine formula, in radians
	 *
	 * @param toL - the other location
	 * @return distance in radians
	 */
	public double distanceRadiansTo(Location toL) {
		return GEO.getDistCalc().distance(point, toL.getPoint());
	}

	/**
	 * Distance between this location and the given one, as determined by the Haversine formula, in km
	 *
	 * @param toL - the other location
	 * @return distance in km
	 */
	public double distanceKmTo(Location toL) {
		return distanceRadiansTo(toL) * DEG_TO_KM;
	}

	/*****************************************************************
	 * Getters and String
	 ****************************************************************/

	@JsonIgnore
	public Point getPoint() {
		return point;
	}

	@JsonIgnore
	public double getLat() {
		return point.getLat();
	}

	@JsonIgnore
	public double getLon() {
		return point.getLon();
	}

	@JsonProperty("WKT")
	public String getWKTString() {
		ShapeWriter writer = GEO.getFormats().getWktWriter();
		return writer.toString(point);
	}

	@Override
	public String toString() {
		return getWKTString();
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

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

	public static void main(String[] args) {
		Location l = new Location(39.984702, 116.318417);
		Location l2 = new Location(39.974702, 116.318417);
		logger.info("Distance is {}km", l.distanceKmTo(l2));
	}

}
