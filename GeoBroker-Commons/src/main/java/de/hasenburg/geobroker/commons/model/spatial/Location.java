package de.hasenburg.geobroker.commons.model.spatial;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.model.JSONable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.shape.Point;

import java.text.ParseException;
import java.util.Objects;
import java.util.Random;

import static de.hasenburg.geobroker.commons.model.spatial.SpatialContext.GEO;
import static org.locationtech.spatial4j.distance.DistanceUtils.DEG_TO_KM;
import static org.locationtech.spatial4j.distance.DistanceUtils.KM_TO_DEG;

public class Location implements JSONable {

	private static final Logger logger = LogManager.getLogger();

	private final Point point;

	private boolean undefined = false;

	private Location(Point point) {
		this.point = point;
	}

	private Location(boolean undefined) {
		this.undefined = undefined;
		this.point = null;
	}

	@JsonCreator
	private Location(@JsonProperty("WKT") String wkt) throws ParseException {
		if ("{ undefined }".equals(wkt)) {
			this.point = null;
			this.undefined = true;
		} else {
			WKTReader reader = (WKTReader) GEO.getFormats().getWktReader();
			this.point = (Point) reader.parse(wkt);
		}
	}

	/**
	 * Creates a location with the given lat/lon coordinates.
	 *
	 * @param lat - the latitude (Breitengrad)
	 * @param lon - the longitude (Längengrad)
	 */
	public Location(double lat, double lon) {
		point = GEO.getShapeFactory().pointLatLon(lat, lon);
	}

	/**
	 * Creates an undefined location, i.e., one that does not have a point and is not contained in any geofence.
	 */
	@NotNull
	public static Location undefined() {
		return new Location(true);
	}

	/**
	 * Creates a random location (Not inclusive of (-90, 0))
	 */
	public static Location random() {
		Random random = new Random();
		// there have been rounding errors
		return new Location(Math.min((random.nextDouble() * -180.0) + 90.0, 90.0),
				Math.min((random.nextDouble() * -360.0) + 180.0, 180.0));
	}

	/**
	 * Creates a random location that is inside the given Geofence.
	 *
	 * @param geofence - may not be a geofence that crosses any datelines!!
	 * @return a random location or null if the geofence crosses a dateline
	 */
	public static Location randomInGeofence(Geofence geofence) {
		Location result = null;
		int i = 0;
		do {
			// generate lat in bounding box
			double lat = Utility.randomDouble(geofence.getBoundingBoxSouthWest().getLat(),
					geofence.getBoundingBoxNorthEast().getLat());

			// generate lon in bounding box
			double lon = Utility.randomDouble(geofence.getBoundingBoxSouthWest().getLon(),
					geofence.getBoundingBoxNorthEast().getLon());

			// create location and hope it is in geofence
			result = new Location(lat, lon);
		} while (!geofence.contains(result) && ++i < 1000);
		// location was in geofence, so let's return it
		return result;
	}

	/**
	 * @param location - starting location
	 * @param distance - distance from starting location in km
	 * @param direction - direction (0 - 360)
	 */
	public static Location locationInDistance(Location location, double distance, double direction) {
		if (location.isUndefined()) {
			return Location.undefined();
		}
		Point result = GEO.getDistCalc().pointOnBearing(location.point,
				distance * KM_TO_DEG,
				direction,
				GEO,
				GEO.getShapeFactory().pointLatLon(0.0, 0.0));

		return new Location(result);
	}

	/**
	 * Distance between this location and the given one, as determined by the Haversine formula, in radians
	 *
	 * @param toL - the other location
	 * @return distance in radians or -1 if one location is undefined
	 */
	public double distanceRadiansTo(Location toL) {
		if (undefined || toL.undefined) {
			return -1.0;
		}
		return GEO.getDistCalc().distance(point, toL.getPoint());
	}

	/**
	 * Distance between this location and the given one, as determined by the Haversine formula, in km
	 *
	 * @param toL - the other location
	 * @return distance in km or -1 if one location is undefined
	 */
	public double distanceKmTo(Location toL) {
		if (undefined || toL.undefined) {
			return -1.0;
		}
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
		if (undefined) {
			return "{ undefined }";
		}
		ShapeWriter writer = GEO.getFormats().getWktWriter();
		return writer.toString(point);
	}

	@JsonIgnore
	public boolean isUndefined() {
		return undefined;
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
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Location location = (Location) o;
		return undefined == location.undefined && Objects.equals(point, location.point);
	}

	@Override
	public int hashCode() {
		return Objects.hash(point, undefined);
	}

	public static void main(String[] args) {
		Location l = new Location(39.984702, 116.318417);
		Location l2 = new Location(39.974702, 116.318417);
		logger.info("Distance is {}km", l.distanceKmTo(l2));

		l = new Location(57.34922076607738, 34.53035122251791);
		l2 = new Location(57.34934475583778, 34.53059311887825);
		logger.info("Distance is {}km", l.distanceKmTo(l2));
	}
}
