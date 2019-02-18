package de.hasenburg.geobroker.commons.model.spatial;

import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.jts.JtsWKTWriter;

import java.text.NumberFormat;
import java.util.Locale;

public class WKTWriter extends JtsWKTWriter {

	public WKTWriter(JtsSpatialContext ctx, JtsSpatialContextFactory factory) {
		super(ctx, factory);
	}

	/**
	 * Same as {@link org.locationtech.spatial4j.io.LegacyShapeWriter#makeNumberFormat(int)}, but does not remove
	 * fraction digits
	 */
	@Override
	protected NumberFormat getNumberFormat() {
		NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);//not thread-safe
		nf.setGroupingUsed(false);
		nf.setMaximumFractionDigits(20); // maximum double is ~16, so we are fine with 20
		nf.setMinimumFractionDigits(0);
		return nf;
	}
}
