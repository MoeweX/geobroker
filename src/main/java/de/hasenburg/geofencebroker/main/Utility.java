package de.hasenburg.geofencebroker.main;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.model.*;
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

	public static Payload buildPayloadFromString(String s, ControlPacketType controlPacketType) {
		switch (controlPacketType) {
			case PINGREQ:
				return JSONable.fromJSON(s, PayloadPINGREQ.class).orElseGet(PayloadPINGREQ::new);
			default:
				// all messages that have only have a reason code can use the standard payload
				return JSONable.fromJSON(s, Payload.class).orElseGet(Payload::new);
		}
	}

}
