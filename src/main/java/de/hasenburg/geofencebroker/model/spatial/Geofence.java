package de.hasenburg.geofencebroker.model.spatial;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hasenburg.geofencebroker.model.JSONable;
import de.hasenburg.geofencebroker.model.exceptions.RuntimeShapeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.SpatialRelation;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static de.hasenburg.geofencebroker.model.spatial.SpatialContext.GEO;


public class Geofence implements JSONable  {

	private static final Logger logger = LogManager.getLogger();
	@JsonIgnore
	private final Shape shape;

	private Geofence(Shape shape) {
		this.shape = shape;
	}

	@JsonCreator
	private Geofence(@JsonProperty("WKT") String wkt) throws ParseException {
		WKTReader reader = (WKTReader) GEO.getFormats().getWktReader();
		this.shape = reader.parse(wkt);
	}

	/**
	 * Creates a new geofence based on the supplied locations
	 *
	 * @param surroundingLocations - the locations that surround the geofence
	 * @return a new geofence
	 * @throws RuntimeShapeException if invalid param
	 */
	public static Geofence polygon(List<Location> surroundingLocations) {
		if (surroundingLocations.size() < 3) {
			throw new RuntimeShapeException("A geofence needs at least 3 locations");
		}

		final ShapeFactory.PolygonBuilder polygonBuilder = GEO.getShapeFactory().polygon();
		for (Location location : surroundingLocations) {
			polygonBuilder.pointLatLon(location.getLat(), location.getLon());
		}
		// close polygon
		polygonBuilder.pointLatLon(surroundingLocations.get(0).getLat(), surroundingLocations.get(0).getLon());
		return new Geofence(polygonBuilder.build());
	}

	public boolean isRectangle() {
		return GEO.getShapeFactory().getGeometryFrom(shape).isRectangle();
	}

	public boolean contains(Location location) {
		return shape.relate(location.getPoint()).equals(SpatialRelation.CONTAINS);
	}

	/*****************************************************************
	 * Getters and String
	 ****************************************************************/

	@JsonProperty("WKT")
	public String getWKTString() {
		ShapeWriter writer = GEO.getFormats().getWktWriter();
		return writer.toString(shape);
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
		Geofence geofence = (Geofence) o;
		return Objects.equals(shape, geofence.shape);
	}

	@Override
	public int hashCode() {

		return Objects.hash(shape);
	}
}
