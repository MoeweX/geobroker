package de.hasenburg.geobroker.server.storage

import de.hasenburg.geobroker.commons.exceptions.RuntimeStorageException
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.LogManager
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.floor
import kotlin.math.roundToLong

private val logger = LogManager.getLogger()

/**
 * Creates a new Raster. The index of each raster entry is the location of the entry's south west corner
 *
 * The raster always contains the whole world. Limitations to allowed subscriptions, e.g., the server should only
 * accept subscriptions from Europe, have to be made on another level
 *
 * The granularity is used to calculate the size of each [RasterEntry] in degrees. The size equals 1 degree /
 * granularity
 *
 * @param granularity - must be >= 1
 * @throws RuntimeStorageException if granularity < 1
 */
class Raster(val granularity: Int) {

    private val rasterEntries = ConcurrentHashMap<Location, RasterEntry>()
    private val worldSubscriptionIds = ConcurrentHashMap<String, MutableSet<ImmutablePair<String, Int>>>()

    private val degreeStep: Double // = 1 / granularity

    val numberOfExistingRasterEntries: Int
        get() = rasterEntries.size

    init {
        if (granularity < 1) {
            throw RuntimeStorageException("Granularity must be >= 1, is $granularity")
        }
        this.degreeStep = 1.0 / granularity
        logger.debug("Raster created, granularity = {}, degree step = {}", granularity, degreeStep)
    }

    /*****************************************************************
     * Subscribe/Unsubscribe Operations
     ****************************************************************/

    /**
     * Adds a subscriptionId to the fitting [RasterEntry]s
     *
     * @param geofence - the geofence used to calculate the fitting [RasterEntry]
     * @param subscriptionId - the subscriptionId to be added
     */
    fun putSubscriptionIdIntoRasterEntries(geofence: Geofence, subscriptionId: ImmutablePair<String, Int>) {
        // add to worldSubscriptionIds if geofence is world
        if (geofence == Geofence.world()) {
            worldSubscriptionIds.getOrPut(subscriptionId.left) { ConcurrentHashMap.newKeySet() }.add(subscriptionId)
            return
        }

        // get all RasterEntries to which the id should be added
        val viableRasterEntries = calculateIndexLocations(geofence)

        // add the subscriptionId to the RasterEntries
        for (viableRasterEntry in viableRasterEntries) {
            viableRasterEntry.putSubscriptionId(subscriptionId)
        }
    }

    /**
     * Removes a subscriptionId from the fitting [RasterEntry]s. Probably used for unsubscribe operations.
     *
     * @param geofence - the geofence used to calculate the fitting [RasterEntry]
     * @param subscriptionId - the subscription id to be removed
     */
    fun removeSubscriptionIdFromRasterEntries(geofence: Geofence, subscriptionId: ImmutablePair<String, Int>) {
        // remove from worldSubscriptionIds if geofence is world
        if (geofence == Geofence.world()) {
            worldSubscriptionIds[subscriptionId.left]?.remove(subscriptionId)
            return
        }

        // get all RasterEntries from which the id should be removed
        val viableRasterEntries = calculateIndexLocations(geofence)

        // remove the subscriptionId from the RasterEntries
        for (viableRasterEntry in viableRasterEntries) {
            viableRasterEntry.removeSubscriptionId(subscriptionId)
        }
    }

    /**
     * Removes a subscriptionId from a single [RasterEntry]. Probably used when a subscription is renewed with a
     * new geofence and outdated subscription ids need to be removed.
     * If no subscription existed, does nothing.
     *
     * @param index - index of [RasterEntry] from which the subscription id should be removed
     * @param subscriptionId - the subscription id to be removed
     */
    fun removeSubscriptionIdFromRasterEntry(index: Location, subscriptionId: ImmutablePair<String, Int>) {
        rasterEntries[index]?.removeSubscriptionId(subscriptionId)
    }

    /*****************************************************************
     * Process Published Message Operations
     ****************************************************************/

    /**
     * Returns all subscriptionIds that are in the fitting [RasterEntry]. In case no subscription ids exist, the
     * returned Set is empty.
     *
     * Also returns all world subscription ids as the publisher location is in all of these.
     *
     * Note: while unlikely, the returned list might contain some duplicates
     *
     * @param location - the location that determines which [RasterEntry] fits
     * @return a set containing all fitting subscriptionIds
     */
    fun getSubscriptionIdsInRasterEntryForPublisherLocation(location: Location): List<ImmutablePair<String, Int>> {
        val index = calculateIndexLocation(location)

        val result = mutableListOf<ImmutablePair<String, Int>>()

        for (rasterEntry in rasterEntries[index]?.allSubscriptionIds?.values ?: emptyList()) {
            result.addAll(rasterEntry)
        }

        for (worldIds in worldSubscriptionIds.values) {
            result.addAll(worldIds)
        }

        return result
    }

    /*****************************************************************
     * Methods to calculate Indices
     ****************************************************************/

    /**
     * Calculates at what index the corresponding [RasterEntry] is stored
     *
     * @param location - the location
     * @return - the index
     */
    private fun calculateIndexLocation(location: Location): Location {
        val latIndex = floor(location.lat * granularity) / granularity
        val lonIndex = floor(location.lon * granularity) / granularity

        return Location(latIndex, lonIndex)
    }

    /**
     * Calculates with which [RasterEntry] the given geofence intersects with.
     *
     * @param geofence - the geofence
     * @return - a list of [RasterEntry]s
     */
    private fun calculateIndexLocations(geofence: Geofence): List<RasterEntry> {
        // get north east and south west indices
        val northEastIndex = calculateIndexLocation(geofence.boundingBoxNorthEast)
        val southWestIndex = calculateIndexLocation(geofence.boundingBoxSouthWest)

        // get raster entries that have to be checked for intersection
        val rasterEntriesToCheckForIntersection = ArrayList<RasterEntry>()
        // due to double precision, we need to check whether > -0.000000001
        var lat = southWestIndex.lat
        while (northEastIndex.lat - lat > -0.000000001) {
            var lon = southWestIndex.lon
            while (northEastIndex.lon - lon > -0.000000001) {
                lat = (lat * granularity).roundToLong() / granularity.toDouble()
                lon = (lon * granularity).roundToLong() / granularity.toDouble()
                val index = Location(lat, lon)
                val re = rasterEntries.getOrPut(index) { RasterEntry(index, degreeStep) }
                rasterEntriesToCheckForIntersection.add(re)
                lon += degreeStep
            }
            lat += degreeStep
        }

//        // if geofence is a rectangle, we can collect the indices
//        // doing the isRectangle check is too expensive with spatial4j to be worth it
//        if (geofence.isRectangle) {
//            return rasterEntriesToCheckForIntersection
//        }

        // remove raster entries whose box is disjoint with the actual geofence
        rasterEntriesToCheckForIntersection.removeIf { re -> re.rasterEntryBox.disjoint(geofence) }

        // return
        return rasterEntriesToCheckForIntersection
    }

    /*****************************************************************
     * No Geo-Context (used to calculate Overhead)
     *
     * In order to use the storage without geo-context information,
     * the two private methods above need to be commented out.
     *
     * Note: the below is still Java code
     ****************************************************************/

//    	private final Location index = new Location(0.0, 0.0);
//
//    	private Location calculateIndexLocation(Location location) {
//    		return index;
//    	}
//
//    	/**
//    	 * Calculates with which {@link RasterEntry} the given geofence intersects with.
//    	 *
//    	 * @param geofence - the geofence
//    	 * @return - a list of {@link RasterEntry}s
//    	 */
//    	private List<RasterEntry> calculateIndexLocations(Geofence geofence) {
//    		List<RasterEntry> list = new ArrayList<>();
//    		list.add(rasterEntries.computeIfAbsent(index, k -> new RasterEntry(index, degreeStep))); // only one entry
//    		return list;
//    	}

}
