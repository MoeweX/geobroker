package de.hasenburg.geobroker.commons.model.spatial

import org.locationtech.spatial4j.context.SpatialContext
import org.locationtech.spatial4j.context.SpatialContextFactory
import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory
import org.locationtech.spatial4j.shape.Shape
import java.text.NumberFormat
import java.text.ParseException
import java.util.*

object SpatialContextK {
    val GEO: JtsSpatialContext

    /**
     * Overwrite default readers with customized ones
     */
    init {
        val factory = JtsSpatialContextFactory()
        factory.geo = true
        factory.readers.clear()
        factory.readers.add(CustomWKTReader::class.java)
        factory.writers.clear()
        factory.writers.add(CustomWKTWriter::class.java)
        GEO = JtsSpatialContext(factory)
    }
}

/**
 * @see org.locationtech.spatial4j.io.WKTReader
 */
class CustomWKTReader(ctx: SpatialContext, factory: SpatialContextFactory) :
    org.locationtech.spatial4j.io.WKTReader(ctx, factory) {

    /**
     * Mostly similar to [org.locationtech.spatial4j.io.WKTReader.parsePolygonShape],
     * but does not create a rect if polygon has rect shape.
     */
    @Throws(ParseException::class)
    override fun parsePolygonShape(state: State): Shape {
        var polygonBuilder = shapeFactory.polygon()
        if (!state.nextIfEmptyAndSkipZM()) {
            polygonBuilder = polygon(state, polygonBuilder)
        }
        return polygonBuilder.build()
    }
}

/**
 * @see org.locationtech.spatial4j.io.jts.JtsWKTWriter
 */
class CustomWKTWriter(ctx: JtsSpatialContext?, factory: JtsSpatialContextFactory?) :
    org.locationtech.spatial4j.io.jts.JtsWKTWriter(ctx, factory) {

    /**
     * Same as [org.locationtech.spatial4j.io.LegacyShapeWriter.makeNumberFormat], but does not remove
     * fraction digits
     */
    override fun getNumberFormat(): NumberFormat {
        val nf = NumberFormat.getInstance(Locale.ROOT) //not thread-safe
        nf.isGroupingUsed = false
        nf.maximumFractionDigits = 20 // maximum double is ~16, so we are fine with 20
        nf.minimumFractionDigits = 0
        return nf
    }
}