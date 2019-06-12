package de.hasenburg.geobroker.commons.model.message.payloads

import de.hasenburg.geobroker.commons.model.spatial.Location

import java.util.Objects

class BrokerForwardPublishPayload : AbstractPayload {

    val publishPayload: PUBLISHPayload
    val publisherLocation: Location // needed in case of matching at the subscriber
    var subscriberClientIdentifiers: List<String> // needed in case of matching at the publisher

    /**
     * This constructor is used in case of matching at the subscriber
     */
    constructor(publishPayload: PUBLISHPayload, publisherLocation: Location) : super() {
        this.publishPayload = publishPayload
        this.publisherLocation = publisherLocation
        this.subscriberClientIdentifiers = emptyList()
    }

    /**
     * This constructor is used in case of matching at the publisher
     */
    constructor(publishPayload: PUBLISHPayload, subscriberClientIdentifiers: List<String>) : super() {
        this.publishPayload = publishPayload
        this.publisherLocation = Location.undefined()
        this.subscriberClientIdentifiers = subscriberClientIdentifiers
    }

    /**
     * This constructor is used by [de.hasenburg.geobroker.commons.model.KryoSerializer] as kryo does not know
     * which kind of matching we are doing.
     */
    constructor(publishPayload: PUBLISHPayload, subscriberClientIdentifiers: List<String>,
                publisherLocation: Location) : super() {
        this.publishPayload = publishPayload
        this.publisherLocation = publisherLocation
        this.subscriberClientIdentifiers = subscriberClientIdentifiers
    }

    /*****************************************************************
     * Generated methods
     ****************************************************************/

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BrokerForwardPublishPayload

        if (publishPayload != other.publishPayload) return false
        if (publisherLocation != other.publisherLocation) return false
        if (subscriberClientIdentifiers != other.subscriberClientIdentifiers) return false

        return true
    }

    override fun hashCode(): Int {
        var result = publishPayload.hashCode()
        result = 31 * result + publisherLocation.hashCode()
        result = 31 * result + subscriberClientIdentifiers.hashCode()
        return result
    }

    override fun toString(): String {
        return "BrokerForwardPublishPayload(publishPayload=$publishPayload, publisherLocation=$publisherLocation, " + "subscriberClientIdentifiers=$subscriberClientIdentifiers)"
    }


}
