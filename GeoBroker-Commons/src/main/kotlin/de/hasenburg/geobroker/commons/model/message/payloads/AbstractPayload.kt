package de.hasenburg.geobroker.commons.model.message.payloads

/**
 * Every [InternalServerMessage], [InternalBrokerMessage] and [InternalClientMessage] has at least this empty payload.
 */
open class AbstractPayload {

    /*****************************************************************
     * Subclasses
     ****************************************************************/

    fun getCONNECTPayload(): CONNECTPayload? {
        if (this is CONNECTPayload) return this else return null
    }

    fun getCONNACKPayload(): CONNACKPayload? {
        if (this is CONNACKPayload) return this else return null
    }

    fun getDISCONNECTPayload(): DISCONNECTPayload? {
        if (this is DISCONNECTPayload) return this else return null
    }

    fun getPINGREQPayload(): PINGREQPayload? {
        if (this is PINGREQPayload) return this else return null
    }

    fun getPINGRESPPayload(): PINGRESPPayload? {
        if (this is PINGRESPPayload) return this else return null
    }

    fun getPUBLISHPayload(): PUBLISHPayload? {
        if (this is PUBLISHPayload) return this else return null
    }

    fun getPUBACKPayload(): PUBACKPayload? {
        if (this is PUBACKPayload) return this else return null
    }

    fun getSUBSCRIBEPayload(): SUBSCRIBEPayload? {
        if (this is SUBSCRIBEPayload) return this else return null
    }

    fun getSUBACKPayload(): SUBACKPayload? {
        if (this is SUBACKPayload) return this else return null
    }

    fun getUNSUBSCRIBEPayload(): UNSUBSCRIBEPayload? {
        if (this is UNSUBSCRIBEPayload) return this else return null
    }

    fun getUNSUBACKPayload(): UNSUBACKPayload? {
        if (this is UNSUBACKPayload) return this else return null
    }

    fun getBrokerForwardDisconnectPayload(): BrokerForwardDisconnectPayload? {
        if (this is BrokerForwardDisconnectPayload) return this else return null
    }

    fun getBrokerForwardPingreqPayload(): BrokerForwardPingreqPayload? {
        if (this is BrokerForwardPingreqPayload) return this else return null
    }

    fun getBrokerForwardSubscribePayload(): BrokerForwardSubscribePayload? {
        if (this is BrokerForwardSubscribePayload) return this else return null
    }

    fun getBrokerForwardUnsubscribePayload(): BrokerForwardUnsubscribePayload? {
        if (this is BrokerForwardUnsubscribePayload) return this else return null
    }

    fun getBrokerForwardPublishPayload(): BrokerForwardPublishPayload? {
        if (this is BrokerForwardPublishPayload) return this else return null
    }

}
