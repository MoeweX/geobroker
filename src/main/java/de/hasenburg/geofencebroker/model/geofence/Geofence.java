package de.hasenburg.geofencebroker.model.geofence;

import de.hasenburg.geofencebroker.model.JSONable;
import de.hasenburg.geofencebroker.model.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Objects;
import java.util.Optional;

public class Geofence {

	private static final Logger logger = LogManager.getLogger();

	public enum Shape {
		EMPTY,
		CIRCLE
	}

	private Shape shape = Shape.EMPTY;

	/*****************************************************************
	 * Geofence Circle
	 ****************************************************************/

	private GeofenceCIRCLE geofenceCIRCLE;

	public void buildCircle(Location location, Double diameterInMeter) {
		if (location == null || diameterInMeter == null) {
			shape = Shape.EMPTY;
			logger.warn("Failed to build circle as location or diameter is null.");
			return;
		}
		shape = Shape.CIRCLE;
		geofenceCIRCLE = new GeofenceCIRCLE(location, diameterInMeter);
	}

	/**
	 * -> If empty, the shape of the geofence is incompatible
	 * -> If not empty, all fields of {@link GeofenceCIRCLE} are guaranteed to be not null.
	 */
	public Optional<GeofenceCIRCLE> getGeofenceCircle() {
		if (Shape.CIRCLE == shape) {
			logger.warn("Cannot return geofence circle if shape is {}", shape.name());
			return Optional.of(geofenceCIRCLE);
		}
		return Optional.empty();
	}

	/*****************************************************************
	 * JSON and toString
	 ****************************************************************/

	public static Geofence fromJSON(String json) {

		Geofence geofence = new Geofence();

		try {
			JSONObject obj = new JSONObject(json);
			// get shape
			Shape shape = Shape.valueOf(obj.getString("shape"));

			// if shape is invalid or empty, we do not have to do anything here
			switch (shape) {
				case CIRCLE:
					Optional<GeofenceCIRCLE> circle = JSONable.fromJSON(obj.getJSONObject("data").toString(), GeofenceCIRCLE.class);
					circle.ifPresent(geofenceCIRCLE1 -> geofence.buildCircle(
							geofenceCIRCLE1.getCircleLocation(),
							geofenceCIRCLE1.getCircleDiameterInMeter()));
			}

		} catch (JSONException e) {
			logger.warn("JSON {} is not valid, returning empty fence", json);
		}

		return geofence;
	}

	private String getCompleteJSON(JSONable object) {
		JSONObject obj = new JSONObject();
		obj.put("shape", shape);
		if (object != null) {
			obj.put("data", new JSONObject(JSONable.toJSON(object)));
		}
		return obj.toString();
	}

	public String toJSON() {
		switch (shape) {
			case CIRCLE:
				return getCompleteJSON(geofenceCIRCLE);
			case EMPTY:
				return getCompleteJSON(null);
			default:
				logger.error("Should never have no shape set.");
				return getCompleteJSON(null);
		}
	}

	@Override
	public String toString() {
		return toJSON();
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	public Shape getShape() {
		return shape;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Geofence)) {
			return false;
		}
		Geofence geofence = (Geofence) o;
		return getShape() == geofence.getShape() &&
				Objects.equals(geofenceCIRCLE, geofence.geofenceCIRCLE);
	}

	@Override
	public int hashCode() {

		return Objects.hash(getShape(), geofenceCIRCLE);
	}
}
