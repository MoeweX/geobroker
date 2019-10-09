package de.hasenburg.geobroker.commons.model

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import de.hasenburg.geobroker.commons.model.message.ControlPacketType
import de.hasenburg.geobroker.commons.model.message.ReasonCode
import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.message.payloads.*
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location


class KryoSerializer {
    val kryo = Kryo()
    private val output = Output(1024, -1)
    private val input = Input()

    /**
     * Specifying new customised serialisers for kryo to work on our different payloads
     */
    init {
        kryo.register(ReasonCode::class.java)

        kryo.register(BrokerInfo::class.java, object : Serializer<BrokerInfo>() {

            override fun write(kryo: Kryo, output: Output, o: BrokerInfo) {
                kryo.writeObjectOrNull(output, o.brokerId, String::class.java)
                kryo.writeObjectOrNull(output, o.ip, String::class.java)
                kryo.writeObjectOrNull(output, o.port, Int::class.javaPrimitiveType)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out BrokerInfo>): BrokerInfo? {
                val brokerId = kryo.readObjectOrNull(input, String::class.java) ?: return null
                val ip = kryo.readObjectOrNull(input, String::class.java) ?: return null
                val port = kryo.readObjectOrNull(input, Int::class.javaPrimitiveType!!) ?: return null
                return BrokerInfo(brokerId, ip, port)
            }
        })
        kryo.register(BrokerArea::class.java, object : Serializer<BrokerArea>() {
            override fun write(kryo: Kryo, output: Output, o: BrokerArea) {
                kryo.writeObjectOrNull(output, o.responsibleBroker, BrokerInfo::class.java)
                kryo.writeObjectOrNull(output, o.coveredArea, Geofence::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out BrokerArea>): BrokerArea? {
                val broker = kryo.readObjectOrNull(input, BrokerInfo::class.java) ?: return null
                val geofence = kryo.readObjectOrNull(input, Geofence::class.java) ?: return null
                return BrokerArea(broker, geofence)
            }
        })
        kryo.register(Location::class.java, object : Serializer<Location>() {
            override fun write(kryo: Kryo, output: Output, o: Location) {
                if (o.isUndefined) {
                    kryo.writeObjectOrNull(output, -1000.0, Double::class.javaPrimitiveType)
                    kryo.writeObjectOrNull(output, -1000.0, Double::class.javaPrimitiveType)
                } else {
                    kryo.writeObjectOrNull(output, o.lat, Double::class.javaPrimitiveType)
                    kryo.writeObjectOrNull(output, o.lon, Double::class.javaPrimitiveType)
                }
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out Location>): Location? {
                val lat = kryo.readObjectOrNull(input, Double::class.javaPrimitiveType!!) ?: return null
                val lon = kryo.readObjectOrNull(input, Double::class.javaPrimitiveType!!) ?: return null
                return if (lat == -1000.0 && lon == -1000.0) {
                    Location(true)
                } else {
                    Location(lat, lon)
                }
            }
        })
        kryo.register(Geofence::class.java, object : Serializer<Geofence>() {
            override fun write(kryo: Kryo, output: Output, o: Geofence) {
                kryo.writeObjectOrNull(output, o.wktString, String::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out Geofence>): Geofence? {
                try {
                    val str = kryo.readObjectOrNull(input, String::class.java) ?: return null
                    return Geofence(str)
                } catch (ex: Exception) {
                    return null
                }

            }
        })
        kryo.register(Topic::class.java, object : Serializer<Topic>() {
            override fun write(kryo: Kryo, output: Output, o: Topic) {
                kryo.writeObjectOrNull(output, o.topic, String::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out Topic>): Topic? {
                val str = kryo.readObjectOrNull(input, String::class.java) ?: return null
                return Topic(str)
            }
        })
        kryo.register(BrokerForwardDisconnectPayload::class.java,
                object : Serializer<BrokerForwardDisconnectPayload>() {
                    override fun write(kryo: Kryo, output: Output, o: BrokerForwardDisconnectPayload) {
                        kryo.writeObjectOrNull(output, o.clientIdentifier, String::class.java)
                        kryo.writeObjectOrNull(output, o.disconnectPayload, DISCONNECTPayload::class.java)
                    }

                    override fun read(kryo: Kryo, input: Input,
                                      aClass: Class<out BrokerForwardDisconnectPayload>): BrokerForwardDisconnectPayload? {
                        val clientIdentifier = kryo.readObjectOrNull(input, String::class.java) ?: return null
                        val disconnectPayload =
                                kryo.readObjectOrNull(input, DISCONNECTPayload::class.java) ?: return null
                                        ?: return null
                        return BrokerForwardDisconnectPayload(
                                clientIdentifier,
                                disconnectPayload)
                    }
                })
        kryo.register(BrokerForwardPingreqPayload::class.java, object : Serializer<BrokerForwardPingreqPayload>() {
            override fun write(kryo: Kryo, output: Output, o: BrokerForwardPingreqPayload) {
                kryo.writeObjectOrNull(output, o.clientIdentifier, String::class.java)
                kryo.writeObjectOrNull(output, o.pingreqPayload, PINGREQPayload::class.java)
            }

            override fun read(kryo: Kryo, input: Input,
                              aClass: Class<out BrokerForwardPingreqPayload>): BrokerForwardPingreqPayload? {
                val clientIdentifier = kryo.readObjectOrNull(input, String::class.java) ?: return null
                val pingreqPayload = kryo.readObjectOrNull(input, PINGREQPayload::class.java) ?: return null
                return de.hasenburg.geobroker.commons.model.message.payloads.BrokerForwardPingreqPayload(
                        clientIdentifier,
                        pingreqPayload)
            }
        })
        kryo.register(BrokerForwardPublishPayload::class.java, object : Serializer<BrokerForwardPublishPayload>() {
            override fun write(kryo: Kryo, output: Output, o: BrokerForwardPublishPayload) {
                kryo.writeObjectOrNull(output, o.publishPayload, PUBLISHPayload::class.java)
                kryo.writeObjectOrNull(output, o.publisherLocation, Location::class.java)
                for (subscriberClientIdentifier in o.subscriberClientIdentifiers) {
                    kryo.writeObjectOrNull(output, subscriberClientIdentifier, String::class.java)
                }
            }

            override fun read(kryo: Kryo, input: Input,
                              aClass: Class<out BrokerForwardPublishPayload>): BrokerForwardPublishPayload? {
                val publishPayload = kryo.readObjectOrNull(input, PUBLISHPayload::class.java) ?: return null
                val location = kryo.readObjectOrNull(input, Location::class.java) ?: return null
                val subscriberClientIdentifiers = mutableListOf<String>()
                while (!input.end()) {
                    val sci = kryo.readObjectOrNull(input, String::class.java)
                    if (sci != null) {
                        subscriberClientIdentifiers.add(sci)
                    }
                }
                return BrokerForwardPublishPayload(publishPayload, subscriberClientIdentifiers, location)
            }
        })
        kryo.register(BrokerForwardSubscribePayload::class.java, object : Serializer<BrokerForwardSubscribePayload>() {
            override fun write(kryo: Kryo, output: Output, o: BrokerForwardSubscribePayload) {
                kryo.writeObjectOrNull(output, o.clientIdentifier, String::class.java)
                kryo.writeObjectOrNull(output, o.subscribePayload, SUBSCRIBEPayload::class.java)
            }

            override fun read(kryo: Kryo, input: Input,
                              aClass: Class<out BrokerForwardSubscribePayload>): BrokerForwardSubscribePayload? {
                val clientIdentifier = kryo.readObjectOrNull(input, String::class.java) ?: return null
                val subscribePayload = kryo.readObjectOrNull(input, SUBSCRIBEPayload::class.java) ?: return null
                return BrokerForwardSubscribePayload(clientIdentifier, subscribePayload)
            }
        })
        kryo.register(BrokerForwardUnsubscribePayload::class.java,
                object : Serializer<BrokerForwardUnsubscribePayload>() {
                    override fun write(kryo: Kryo, output: Output, o: BrokerForwardUnsubscribePayload) {
                        kryo.writeObjectOrNull(output, o.clientIdentifier, String::class.java)
                        kryo.writeObjectOrNull(output, o.unsubscribePayload, UNSUBSCRIBEPayload::class.java)
                    }

                    override fun read(kryo: Kryo, input: Input,
                                      aClass: Class<out BrokerForwardUnsubscribePayload>): BrokerForwardUnsubscribePayload? {
                        val clientIdentifier = kryo.readObjectOrNull(input, String::class.java) ?: return null
                        val unsubscribePayload =
                                kryo.readObjectOrNull(input, UNSUBSCRIBEPayload::class.java) ?: return null
                        return BrokerForwardUnsubscribePayload(clientIdentifier, unsubscribePayload)
                    }
                })
        kryo.register(CONNACKPayload::class.java, object : Serializer<CONNACKPayload>() {
            override fun write(kryo: Kryo, output: Output, o: CONNACKPayload) {
                kryo.writeObjectOrNull(output, o.reasonCode, ReasonCode::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out CONNACKPayload>): CONNACKPayload? {
                val reasonCode = kryo.readObjectOrNull(input, ReasonCode::class.java) ?: return null
                return CONNACKPayload(reasonCode)
            }
        })
        kryo.register(CONNECTPayload::class.java, object : Serializer<CONNECTPayload>() {
            override fun write(kryo: Kryo, output: Output, o: CONNECTPayload) {
                kryo.writeObjectOrNull(output, o.location, Location::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out CONNECTPayload>): CONNECTPayload? {
                val location = kryo.readObjectOrNull(input, Location::class.java) ?: return null
                return CONNECTPayload(location)
            }
        })
        kryo.register(DISCONNECTPayload::class.java, object : Serializer<DISCONNECTPayload>() {
            override fun write(kryo: Kryo, output: Output, o: DISCONNECTPayload) {
                kryo.writeObjectOrNull(output, o.reasonCode, ReasonCode::class.java)
                kryo.writeObjectOrNull(output, o.brokerInfo, BrokerInfo::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out DISCONNECTPayload>): DISCONNECTPayload? {
                val reasonCode = kryo.readObjectOrNull(input, ReasonCode::class.java) ?: return null
                val brokerInfo = kryo.readObjectOrNull(input, BrokerInfo::class.java)
                return DISCONNECTPayload(reasonCode, brokerInfo)
            }
        })
        kryo.register(PINGREQPayload::class.java, object : Serializer<PINGREQPayload>() {
            override fun write(kryo: Kryo, output: Output, o: PINGREQPayload) {
                kryo.writeObjectOrNull(output, o.location, Location::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out PINGREQPayload>): PINGREQPayload? {
                val location = kryo.readObjectOrNull(input, Location::class.java) ?: return null
                return PINGREQPayload(location)
            }
        })
        kryo.register(PINGRESPPayload::class.java, object : Serializer<PINGRESPPayload>() {
            override fun write(kryo: Kryo, output: Output, o: PINGRESPPayload) {
                kryo.writeObjectOrNull(output, o.reasonCode, ReasonCode::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out PINGRESPPayload>): PINGRESPPayload? {
                val reasonCode = kryo.readObjectOrNull(input, ReasonCode::class.java) ?: return null
                return PINGRESPPayload(reasonCode)
            }
        })
        kryo.register(PUBACKPayload::class.java, object : Serializer<PUBACKPayload>() {
            override fun write(kryo: Kryo, output: Output, o: PUBACKPayload) {
                kryo.writeObjectOrNull(output, o.reasonCode, ReasonCode::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out PUBACKPayload>): PUBACKPayload? {
                val reasonCode = kryo.readObjectOrNull(input, ReasonCode::class.java) ?: return null
                return PUBACKPayload(reasonCode)
            }
        })
        kryo.register(PUBLISHPayload::class.java, object : Serializer<PUBLISHPayload>() {
            override fun write(kryo: Kryo, output: Output, o: PUBLISHPayload) {
                kryo.writeObjectOrNull(output, o.content, String::class.java)
                kryo.writeObjectOrNull(output, o.geofence, Geofence::class.java)
                kryo.writeObjectOrNull(output, o.topic, Topic::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out PUBLISHPayload>): PUBLISHPayload? {
                val content = kryo.readObjectOrNull(input, String::class.java) ?: return null
                val g = kryo.readObjectOrNull(input, Geofence::class.java) ?: return null
                val topic = kryo.readObjectOrNull(input, Topic::class.java) ?: return null
                return PUBLISHPayload(topic, g, content)
            }
        })
        kryo.register(SUBACKPayload::class.java, object : Serializer<SUBACKPayload>() {
            override fun write(kryo: Kryo, output: Output, o: SUBACKPayload) {
                kryo.writeObjectOrNull(output, o.reasonCode, ReasonCode::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out SUBACKPayload>): SUBACKPayload? {
                val reasonCode = kryo.readObjectOrNull(input, ReasonCode::class.java) ?: return null
                return SUBACKPayload(reasonCode)
            }
        })
        kryo.register(SUBSCRIBEPayload::class.java, object : Serializer<SUBSCRIBEPayload>() {
            override fun write(kryo: Kryo, output: Output, o: SUBSCRIBEPayload) {
                kryo.writeObjectOrNull(output, o.geofence, Geofence::class.java)
                kryo.writeObjectOrNull(output, o.topic, Topic::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out SUBSCRIBEPayload>): SUBSCRIBEPayload? {
                val geofence = kryo.readObjectOrNull(input, Geofence::class.java) ?: return null
                val topic = kryo.readObjectOrNull(input, Topic::class.java) ?: return null
                return SUBSCRIBEPayload(topic, geofence)
            }
        })
        kryo.register(UNSUBACKPayload::class.java, object : Serializer<UNSUBACKPayload>() {
            override fun write(kryo: Kryo, output: Output, o: UNSUBACKPayload) {
                kryo.writeObjectOrNull(output, o.reasonCode, ReasonCode::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out UNSUBACKPayload>): UNSUBACKPayload? {
                val reasonCode = kryo.readObjectOrNull(input, ReasonCode::class.java) ?: return null
                return UNSUBACKPayload(reasonCode)
            }
        })
        kryo.register(UNSUBSCRIBEPayload::class.java, object : Serializer<UNSUBSCRIBEPayload>() {
            override fun write(kryo: Kryo, output: Output, o: UNSUBSCRIBEPayload) {
                kryo.writeObjectOrNull(output, o.topic, Topic::class.java)
            }

            override fun read(kryo: Kryo, input: Input, aClass: Class<out UNSUBSCRIBEPayload>): UNSUBSCRIBEPayload? {
                val topic = kryo.readObjectOrNull(input, Topic::class.java) ?: return null
                return UNSUBSCRIBEPayload(topic)
            }
        })

    }

    fun write(o: Any): ByteArray {
        kryo.writeObjectOrNull(output, o, o.javaClass)
        val arr = output.toBytes()
        output.reset()
        return arr
    }

    fun <T> read(bytes: ByteArray, targetClass: Class<T>): T {
        input.buffer = bytes
        return kryo.readObjectOrNull(input, targetClass)
    }

    fun read(arr: ByteArray, controlPacketType: ControlPacketType): AbstractPayload? {
        input.buffer = arr
        val o: AbstractPayload
        when (controlPacketType) {
            ControlPacketType.CONNACK -> o = kryo.readObjectOrNull(input, CONNACKPayload::class.java) ?: return null
            ControlPacketType.CONNECT -> o = kryo.readObjectOrNull(input, CONNECTPayload::class.java) ?: return null
            ControlPacketType.DISCONNECT -> o =
                    kryo.readObjectOrNull(input, DISCONNECTPayload::class.java) ?: return null
            ControlPacketType.PINGREQ -> o = kryo.readObjectOrNull(input, PINGREQPayload::class.java) ?: return null
            ControlPacketType.PINGRESP -> o = kryo.readObjectOrNull(input, PINGRESPPayload::class.java) ?: return null
            ControlPacketType.PUBACK -> o = kryo.readObjectOrNull(input, PUBACKPayload::class.java) ?: return null
            ControlPacketType.PUBLISH -> o = kryo.readObjectOrNull(input, PUBLISHPayload::class.java) ?: return null
            ControlPacketType.SUBACK -> o = kryo.readObjectOrNull(input, SUBACKPayload::class.java) ?: return null
            ControlPacketType.SUBSCRIBE -> o = kryo.readObjectOrNull(input, SUBSCRIBEPayload::class.java) ?: return null
            ControlPacketType.UNSUBACK -> o = kryo.readObjectOrNull(input, UNSUBACKPayload::class.java) ?: return null
            ControlPacketType.UNSUBSCRIBE -> o =
                    kryo.readObjectOrNull(input, UNSUBSCRIBEPayload::class.java) ?: return null
            ControlPacketType.BrokerForwardDisconnect -> o =
                    kryo.readObjectOrNull(input, BrokerForwardDisconnectPayload::class.java) ?: return null
            ControlPacketType.BrokerForwardPingreq -> o =
                    kryo.readObjectOrNull(input, BrokerForwardPingreqPayload::class.java) ?: return null
            ControlPacketType.BrokerForwardPublish -> o =
                    kryo.readObjectOrNull(input, BrokerForwardPublishPayload::class.java) ?: return null
            ControlPacketType.BrokerForwardSubscribe -> o =
                    kryo.readObjectOrNull(input, BrokerForwardSubscribePayload::class.java) ?: return null
            ControlPacketType.BrokerForwardUnsubscribe -> o =
                    kryo.readObjectOrNull(input, BrokerForwardUnsubscribePayload::class.java) ?: return null
            else -> return null
        }
        return o
    }
}