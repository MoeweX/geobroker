package de.hasenburg.geobroker.commons.model.spatial;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hasenburg.geobroker.commons.model.JSONable;
import de.hasenburg.geobroker.commons.exceptions.RuntimeShapeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.shape.*;

import java.text.ParseException;
import java.util.List;
import java.util.Objects;

import static de.hasenburg.geobroker.commons.model.spatial.SpatialContext.GEO;


public class Geofence implements JSONable {

	private static final Logger logger = LogManager.getLogger();
	@JsonIgnore
	private final Shape shape;

	// TODO increase size a little bit so that we do not miss any due to rounding issues
	// we need it most times anyways, so let's buffer it //
	@JsonIgnore
	final Rectangle boundingBox;

	private Geofence(Shape shape) {
		this.shape = shape;
		this.boundingBox = shape.getBoundingBox();
	}

	@JsonCreator
	private Geofence(@JsonProperty("WKT") String wkt) throws ParseException {
		WKTReader reader = (WKTReader) GEO.getFormats().getWktReader();
		this.shape = reader.parse(wkt);
		this.boundingBox = this.shape.getBoundingBox();
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

	public static Geofence rectangle(Location southWest, Location northEast) {
		Rectangle r = GEO.getShapeFactory().rect(southWest.getPoint(), northEast.getPoint());
		return new Geofence(r);
	}

	public static Geofence circle(Location location, double radiusDegree) {
		Circle c = GEO.getShapeFactory().circle(location.getPoint(), radiusDegree);
		return new Geofence(c);
	}

	public static Geofence world() {
		Shape worldShape = GEO.getWorldBounds();
		return new Geofence(worldShape);
	}

	@JsonIgnore
	public boolean isRectangle() {
		return GEO.getShapeFactory().getGeometryFrom(shape).isRectangle();
	}

	/*****************************************************************
	 * BoundingBox
	 ****************************************************************/

	@JsonIgnore
	public Location getBoundingBoxNorthWest() {
		return new Location(boundingBox.getMaxY(), boundingBox.getMinX());
	}

	@JsonIgnore
	public Location getBoundingBoxNorthEast() {
		return new Location(boundingBox.getMaxY(), boundingBox.getMaxX());
	}

	@JsonIgnore
	public Location getBoundingBoxSouthEast() {
		return new Location(boundingBox.getMinY(), boundingBox.getMaxX());
	}

	@JsonIgnore
	public Location getBoundingBoxSouthWest() {
		return new Location(boundingBox.getMinY(), boundingBox.getMinX());
	}

	/**
	 * See {@link Rectangle#getHeight()}, is the latitude distance in degree
	 */
	@JsonIgnore
	public double getBoundingBoxLatDistanceInDegree() {
		return boundingBox.getHeight();
	}

	/**
	 * See {@link Rectangle#getWidth()}, is the longitude distance in degree
	 */
	@JsonIgnore
	public double getBoundingBoxLonDistanceInDegree() {
		return boundingBox.getWidth();
	}

	public boolean contains(Location location) {
		return shape.relate(location.getPoint()).equals(SpatialRelation.CONTAINS);
	}

	/**
	 * For us, intersects is an "intersection" but also something more specific such as "contains" or within.
	 */
	public boolean intersects(Geofence geofence) {
		SpatialRelation sr = shape.relate(geofence.shape);
		return sr.equals(SpatialRelation.INTERSECTS) || sr.equals(SpatialRelation.CONTAINS) ||
				sr.equals(SpatialRelation.WITHIN);
	}

	public boolean disjoint(Geofence geofence) {
		return shape.relate(geofence.shape).equals(SpatialRelation.DISJOINT);
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

	public static void main(String[] args) {
		Location paris = new Location(48.86, 2.35);
		Location berlin = new Location(52.52, 13.40);
		Geofence parisArea = Geofence.circle(paris, 3.0);
		Geofence berlinArea = Geofence.circle(berlin, 3.0);

		logger.info("Paris area = {}", parisArea);
		logger.info("Berlin area = {}", berlinArea);
		logger.info("The areas intersect: {}", berlinArea.intersects(parisArea));

		Location justIn = new Location(45.87, 2.3);
		logger.info(parisArea.contains(justIn));
	}
}
