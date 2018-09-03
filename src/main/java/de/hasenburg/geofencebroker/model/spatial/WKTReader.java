package de.hasenburg.geofencebroker.model.spatial;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;

import java.text.ParseException;

public class WKTReader extends org.locationtech.spatial4j.io.WKTReader {

	/**
	 * @see org.locationtech.spatial4j.io.WKTReader
	 */
	public WKTReader(SpatialContext ctx, SpatialContextFactory factory) {
		super(ctx, factory);
	}

	/**
	 * Mostly similar to {@link org.locationtech.spatial4j.io.WKTReader#parsePolygonShape(State)},
	 * but does not create a rect if polygon has rect shape.
	 */
	@Override
	protected Shape parsePolygonShape(State state) throws ParseException {
		ShapeFactory.PolygonBuilder polygonBuilder = shapeFactory.polygon();
		if (!state.nextIfEmptyAndSkipZM()) {
			polygonBuilder = polygon(state, polygonBuilder);
		}
		return polygonBuilder.build();
	}
}
