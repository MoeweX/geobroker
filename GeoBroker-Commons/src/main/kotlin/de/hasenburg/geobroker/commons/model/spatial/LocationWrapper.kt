package de.hasenburg.geobroker.commons.model.spatial

import de.hasenburg.geobroker.commons.model.spatial.LocationWrapper.LocationK
import de.hasenburg.geobroker.commons.model.spatial.SpatialContextK.GEO
import de.hasenburg.geobroker.commons.randomDouble
import kotlinx.serialization.*
import kotlinx.serialization.internal.StringDescriptor
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.distance.DistanceUtils
import org.locationtech.spatial4j.shape.Point
import java.util.*
import kotlin.math.min

private val logger = LogManager.getLogger()

@Serializable
sealed class LocationWrapper {

    @Serializable
    @SerialName("UndefinedLocation")
    object Undefined : LocationWrapper() {
        override fun toString(): String {
            return "undefined"
        }
    }

    // TODO delete Location, rename LocationK to Location
    @Serializable
    @SerialName("LocationK")
    data class LocationK(
            @Serializable(with = PointWKTSerializer::class) @SerialName("wkt") val point: Point
    ) : LocationWrapper() {

        /**
         * Creates a location with the given lat/lon coordinates.
         *
         * @param lat - the latitude (Breitengrad)
         * @param lon - the longitude (Längengrad)
         */
        constructor(lat: Double, lon: Double) : this(GEO.shapeFactory.pointLatLon(lat, lon))

        val lat: Double
            get() = point.lat

        val lon: Double
            get() = point.lon

        /**
         * @param distance - distance from starting location in km
         * @param direction - direction (0 - 360)
         */
        fun locationInDistance(distance: Double, direction: Double): LocationK {
            return LocationK(GEO.distCalc.pointOnBearing(this.point,
                    distance * DistanceUtils.KM_TO_DEG,
                    direction,
                    GEO,
                    GEO.shapeFactory.pointLatLon(0.0, 0.0)))
        }

        /**
         * Distance between this location and the given one, as determined by the Haversine formula, in radians
         *
         * @param toL - the other location
         * @return distance in radians
         */
        fun distanceRadiansTo(toL: LocationK): Double {
            return GEO.distCalc.distance(point, toL.point)
        }

        /**
         * Distance between this location and the given one, as determined by the Haversine formula, in km
         *
         * @param toL - the other location
         * @return distance in km or -1 if one location is undefined
         */
        fun distanceKmTo(toL: LocationK): Double {
            return distanceRadiansTo(toL) * DistanceUtils.DEG_TO_KM
        }

        override fun toString(): String {
            return "Location(lat=$lat, lon=$lon)"
        }

        companion object {
            /**
             * Creates a random location (Not inclusive of (-90, 0))
             */
            fun random(): LocationK {
                val random = Random()
                // there have been rounding errors
                return LocationK(min((random.nextDouble() * -180.0) + 90.0, 90.0),
                        min((random.nextDouble() * -360.0) + 180.0, 180.0))
            }

            /**
             * Creates a random location that is inside the given Geofence.
             *
             * @param geofence - may not be a geofence that crosses any datelines!!
             * @return a random location or null if the geofence crosses a dateline
             */
            fun randomInGeofence(geofence: GeofenceK): LocationK? {
                var result: LocationK
                var i = 0
                do {
                    // generate lat/lon in bounding box
                    val lat = randomDouble(geofence.boundingBoxSouthWest.lat, geofence.boundingBoxNorthEast.lat)
                    val lon = randomDouble(geofence.boundingBoxSouthWest.lon, geofence.boundingBoxNorthEast.lon)

                    // create location and hope it is in geofence
                    result = LocationK(lat, lon)
                } while (!geofence.contains(result) && ++i < 1000)

                return if (geofence.contains(result)) {
                    // location was in geofence, so let's return it
                    result
                } else {
                    // none was found
                    null
                }
            }

        }

    }

}

/*****************************************************************
 * Json Serialization
 ****************************************************************/

fun LocationWrapper.toJson(): String {
    return Json(JsonConfiguration.Stable).stringify(LocationWrapper.serializer(), this)
}

fun String.toLocation(): LocationWrapper {
    return Json(JsonConfiguration.Stable).parse(LocationWrapper.serializer(), this)
}

object PointWKTSerializer : KSerializer<Point> {

    override val descriptor: SerialDescriptor = StringDescriptor

    override fun serialize(encoder: Encoder, obj: Point) {
        encoder.encodeString(GEO.formats.wktWriter.toString(obj))
    }

    override fun deserialize(decoder: Decoder): Point {
        val reader = GEO.formats.wktReader as CustomWKTReader
        return reader.parse(decoder.decodeString()) as Point
    }
}

/*****************************************************************
 * Main to test
 ****************************************************************/

fun main() {
    val ul1 = LocationWrapper.Undefined
    logger.info(ul1)
    val ul1s = ul1.toJson()
    logger.info(ul1s)
    val ul2 = ul1s.toLocation()
    logger.info(ul2)
    assert(ul1 == ul2)

    val l1 = LocationK.random()
    logger.info(l1)
    val l1s = l1.toJson()
    logger.info(l1s)
    val l2 = l1s.toLocation()
    logger.info(l2)
    assert(l1 == l2)

    var a = LocationK(39.984702, 116.318417)
    var b = LocationK(39.974702, 116.318417)
    logger.info("Distance is {}km", a.distanceKmTo(b))

    a = LocationK(57.34922076607738, 34.53035122251791)
    b = LocationK(57.34934475583778, 34.53059311887825)
    logger.info("Distance is {}km", a.distanceKmTo(b))
}