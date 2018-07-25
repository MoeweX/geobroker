package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.model.*;
import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import de.hasenburg.geofencebroker.model.payload.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Utility {

	private static final Logger logger = LogManager.getLogger();

	public static void sleep(long millis, int nanos) {
		try {
			Thread.sleep(millis, nanos);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.error("Interrupted my sleep :S -> interrupting!", e);
		}
	}

	public static void sleepNoLog(long millis, int nanos) {
		try {
			Thread.sleep(millis, nanos);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Returns payload corresponding to the control packet type in the form of an abstract payload.
	 * Checks whether all fields are not null.
	 *
	 * @throws CommunicatorException if control packet type is not supported or a field is null.
	 */
	public static AbstractPayload buildPayloadFromString(String s, ControlPacketType controlPacketType)
			throws CommunicatorException {

		switch (controlPacketType) {
			case CONNACK:
				CONNACKPayload connackPayload =
						JSONable.fromJSON(s, CONNACKPayload.class).orElseGet(CONNACKPayload::new);
				if (!connackPayload.nullField()) {
					return connackPayload;
				}
				break;
			case CONNECT:
				CONNECTPayload connectPayload =
						JSONable.fromJSON(s, CONNECTPayload.class).orElseGet(CONNECTPayload::new);
				if (!connectPayload.nullField()) {
					return connectPayload;
				}
				break;
			case DISCONNECT:
				DISCONNECTPayload disconnectPayload =
						JSONable.fromJSON(s, DISCONNECTPayload.class).orElseGet(DISCONNECTPayload::new);
				if (!disconnectPayload.nullField()) {
					return disconnectPayload;
				}
				break;
			case PINGREQ:
				PINGREQPayload pingreqPayload =
						JSONable.fromJSON(s, PINGREQPayload.class).orElseGet(PINGREQPayload::new);
				if (!pingreqPayload.nullField()) {
					return pingreqPayload;
				}
				break;
			case PINGRESP:
				PINGRESPPayload pingrespPayload =
						JSONable.fromJSON(s, PINGRESPPayload.class).orElseGet(PINGRESPPayload::new);
				if (!pingrespPayload.nullField()) {
					return pingrespPayload;
				}
				break;
			case PUBACK:
				PUBACKPayload pubackPayload =
						JSONable.fromJSON(s, PUBACKPayload.class).orElseGet(PUBACKPayload::new);
				if (!pubackPayload.nullField()) {
					return pubackPayload;
				}
				break;
			case PUBLISH:
				PUBLISHPayload publishPayload =
						JSONable.fromJSON(s, PUBLISHPayload.class).orElseGet(PUBLISHPayload::new);
				if (!publishPayload.nullField()) {
					return publishPayload;
				}
				break;
			case SUBACK:
				SUBACKPayload subackPayload =
						JSONable.fromJSON(s, SUBACKPayload.class).orElseGet(SUBACKPayload::new);
				if (!subackPayload.nullField()) {
					return subackPayload;
				}
				break;
			case SUBSCRIBE:
				SUBSCRIBEPayload subscribePayload =
						JSONable.fromJSON(s, SUBSCRIBEPayload.class).orElseGet(SUBSCRIBEPayload::new);
				if (!subscribePayload.nullField()) {
					return subscribePayload;
				}
				break;
			default:
				throw new CommunicatorException("ControlPacketType " + controlPacketType.name() + " is not supported");
		}

		throw new CommunicatorException("Some of the payloads fields are null.");
	}

}
