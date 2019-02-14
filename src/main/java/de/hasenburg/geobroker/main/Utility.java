package de.hasenburg.geobroker.main;

import de.hasenburg.geobroker.communication.ControlPacketType;
import de.hasenburg.geobroker.model.*;
import de.hasenburg.geobroker.model.exceptions.CommunicatorException;
import de.hasenburg.geobroker.model.payload.*;
import me.atrox.haikunator.Haikunator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.util.Random;

public class Utility {

	private static final Logger logger = LogManager.getLogger();
	private static Random r = new Random();
	private static Haikunator haikunator;
	static {
		haikunator = new Haikunator().setRandom(r);
	}

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
	 * Returns true with the given chance.
	 *
	 * @param chance - the chance to return true (0 - 100)
	 * @return true, if lucky
	 */
	public static boolean getTrueWithChance(int chance) {
		// normalize
		if (chance > 100) {
			chance = 100;
		} else if (chance < 0) {
			chance = 0;
		}
		int random = r.nextInt(100) + 1; // not 0
		return random <= chance;
	}

	/**
	 *
	 * @param bound - int, must be > 0
	 * @return a random int between 0 (inclusive) and bound (exclusive)
	 */
	public static int randomInt(int bound) {
		return r.nextInt(bound);
	}

	public static String randomName() {
		return haikunator.haikunate();
	}

	public static String randomName(Random r) {
		Haikunator h = new Haikunator().setRandom(r);
		h.setRandom(r);
		return h.haikunate();
	}

	/**
	 * Set the logger level of the given logger to the given level.
	 *
	 * @param logger - the logger
	 * @param level - the level
	 */
	public static void setLogLevel(Logger logger, Level level) {
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration conf = ctx.getConfiguration();
		LoggerConfig loggerConfig = conf.getLoggerConfig(logger.getName());
		loggerConfig.setLevel(level);
		ctx.updateLoggers(conf);
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

	public static String generateClientOrderBackendString(String identity) {
		return "inproc://" + identity;
	}

	/**
	 * Generates a string payload with the given size, but the minimum size is length(content) + 8.
	 */
	public static String generatePayloadWithSize(int payloadSize, String content) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(content).append("++++++++");
		for (int i = 0; i < payloadSize - 8 - content.length(); i++) {
			stringBuilder.append("a");
		}
		return stringBuilder.toString();
	}
}
