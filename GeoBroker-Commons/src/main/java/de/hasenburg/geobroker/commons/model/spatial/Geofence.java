package de.hasenburg.geobroker.commons.model.spatial;

import de.hasenburg.geobroker.commons.exceptions.RuntimeShapeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.shape.*;

import java.text.ParseException;
import java.util.List;
import java.util.Objects;

import static de.hasenburg.geobroker.commons.model.spatial.SpatialContext.GEO;


public class Geofence{

	private static final Logger logger = LogManager.getLogger();
	private final Shape shape;
	private boolean undefined = false;

	// TODO increase size a little bit so that we do not miss any due to rounding issues
	// we need it most times anyways, so let's buffer it //
	final Rectangle boundingBox;

	/**
	 * Constructor for an undefined GeoFence.
	 */
	private Geofence() {
		this.undefined = true;
		this.shape = null;
		this.boundingBox = null;
	}

	private Geofence(Shape shape) {
		this.shape = shape;
		this.boundingBox = shape.getBoundingBox();
	}

	public Geofence(String wkt) throws ParseException {
		Rectangle tmpBoundingBox;
		Shape tmpShape;
		WKTReader reader = (WKTReader) GEO.getFormats().getWktReader();
		try {
			tmpShape = reader.parse(wkt);
			tmpBoundingBox = tmpShape.getBoundingBox();
		} catch (ParseException e) {
			tmpShape = null;
			tmpBoundingBox = null;
			this.undefined = true;
		}
		this.boundingBox = tmpBoundingBox;
		this.shape = tmpShape;
	}

	public static Geofence undefined() {
		return new Geofence();
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

	public boolean isRectangle() {
		if (isUndefined()) {
			return false;
		}
		return GEO.getShapeFactory().getGeometryFrom(shape).isRectangle();
	}

	/*****************************************************************
	 * BoundingBox
	 ****************************************************************/

	public Location getBoundingBoxNorthWest() {
		if (isUndefined()) {
			return Location.undefined();
		}
		return new Location(boundingBox.getMaxY(), boundingBox.getMinX());
	}

	public Location getBoundingBoxNorthEast() {
		if (isUndefined()) {
			return Location.undefined();
		}
		return new Location(boundingBox.getMaxY(), boundingBox.getMaxX());
	}

	public Location getBoundingBoxSouthEast() {
		if (isUndefined()) {
			return Location.undefined();
		}
		return new Location(boundingBox.getMinY(), boundingBox.getMaxX());
	}

	public Location getBoundingBoxSouthWest() {
		if (isUndefined()) {
			return Location.undefined();
		}
		return new Location(boundingBox.getMinY(), boundingBox.getMinX());
	}

	/**
	 * See {@link Rectangle#getHeight()}, is the latitude distance in degree
	 */
	public Double getBoundingBoxLatDistanceInDegree() {
		if (isUndefined()) {
			return null;
		}
		return boundingBox.getHeight();
	}

	/**
	 * See {@link Rectangle#getWidth()}, is the longitude distance in degree
	 */
	public Double getBoundingBoxLonDistanceInDegree() {
		if (isUndefined()) {
			return null;
		}
		return boundingBox.getWidth();
	}

	public boolean contains(Location location) {
		if (location.isUndefined() || isUndefined()) {
			return false;
		}
		return shape.relate(location.getPoint()).equals(SpatialRelation.CONTAINS);
	}

	/**
	 * For us, intersects is an "intersection" but also something more specific such as "contains" or within.
	 */
	public boolean intersects(Geofence geofence) {
		if (geofence.isUndefined()) {
			return false;
		}
		SpatialRelation sr = shape.relate(geofence.shape);
		return sr.equals(SpatialRelation.INTERSECTS) || sr.equals(SpatialRelation.CONTAINS) ||
				sr.equals(SpatialRelation.WITHIN);
	}

	public boolean disjoint(Geofence geofence) {
		if (geofence.isUndefined()) {
			return false;
		}
		return shape.relate(geofence.shape).equals(SpatialRelation.DISJOINT);
	}

	/*****************************************************************
	 * Getters and String
	 ****************************************************************/

	public String getWKTString() {
		if (undefined) {
			return "{ undefined }";
		}
		ShapeWriter writer = GEO.getFormats().getWktWriter();
		return writer.toString(shape);
	}

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
		Geofence geofence = (Geofence) o;
		return undefined == geofence.undefined && Objects.equals(shape, geofence.shape) && Objects.equals(boundingBox,
				geofence.boundingBox);
	}

	@Override
	public int hashCode() {
		return Objects.hash(shape, undefined, boundingBox);
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
