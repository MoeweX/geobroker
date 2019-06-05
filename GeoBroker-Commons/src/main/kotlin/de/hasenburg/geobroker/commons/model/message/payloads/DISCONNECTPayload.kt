package de.hasenburg.geobroker.commons.model.message.payloads

import de.hasenburg.geobroker.commons.model.BrokerInfo
import de.hasenburg.geobroker.commons.model.message.ReasonCode

import java.util.Objects

class DISCONNECTPayload(val reasonCode: ReasonCode) : AbstractPayload() {

    var brokerInfo: BrokerInfo? = null

    constructor(reasonCode: ReasonCode, brokerInfo: BrokerInfo?) : this(reasonCode) {
        this.brokerInfo = brokerInfo
    }

    /*****************************************************************
     * Generated methods
     ****************************************************************/

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DISCONNECTPayload

        if (reasonCode != other.reasonCode) return false
        if (brokerInfo != other.brokerInfo) return false

        return true
    }

    override fun hashCode(): Int {
        var result = reasonCode.hashCode()
        result = 31 * result + (brokerInfo?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "DISCONNECTPayload(reasonCode=$reasonCode, brokerInfo=$brokerInfo)"
    }

}
