package de.hasenburg.geofencebroker.model.payload;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.hasenburg.geofencebroker.model.InternalClientMessage;
import de.hasenburg.geofencebroker.model.InternalBrokerMessage;
import de.hasenburg.geofencebroker.model.JSONable;

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * Every {@link InternalClientMessage} or {@link InternalBrokerMessage} has at least this empty payload.
 */
public abstract class AbstractPayload implements JSONable {

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	public boolean nullField() {
		try {
			for (Field f : getClass().getDeclaredFields()) {
				if (f.get(this) == null) {
					return true;
				}
			}
			return false;
		} catch (IllegalAccessException e) {
			logger.error("Exception while null checking fields", e);
			return true;
		}
	}

	/*****************************************************************
	 * Subclasses
	 ****************************************************************/

	@JsonIgnore
	public Optional<CONNACKPayload> getCONNACKPayload() {
		if (this instanceof CONNACKPayload) {
			return Optional.of((CONNACKPayload) this);
		} else {
			return Optional.empty();
		}
	}

	@JsonIgnore
	public Optional<CONNECTPayload> getCONNECTPayload() {
		if (this instanceof CONNECTPayload) {
			return Optional.of((CONNECTPayload) this);
		} else {
			return Optional.empty();
		}
	}

	@JsonIgnore
	public Optional<DISCONNECTPayload> getDISCONNECTPayload() {
		if (this instanceof DISCONNECTPayload) {
			return Optional.of((DISCONNECTPayload) this);
		} else {
			return Optional.empty();
		}
	}

	@JsonIgnore
	public Optional<PINGREQPayload> getPINGREQPayload() {
		if (this instanceof PINGREQPayload) {
			return Optional.of((PINGREQPayload) this);
		} else {
			return Optional.empty();
		}
	}

	@JsonIgnore
	public Optional<PINGRESPPayload> getPINGRESPPayload() {
		if (this instanceof PINGRESPPayload) {
			return Optional.of((PINGRESPPayload) this);
		} else {
			return Optional.empty();
		}
	}

	@JsonIgnore
	public Optional<PUBACKPayload> getPUBACKPayload() {
		if (this instanceof PUBACKPayload) {
			return Optional.of((PUBACKPayload) this);
		} else {
			return Optional.empty();
		}
	}

	@JsonIgnore
	public Optional<PUBLISHPayload> getPUBLISHPayload() {
		if (this instanceof PUBLISHPayload) {
			return Optional.of((PUBLISHPayload) this);
		} else {
			return Optional.empty();
		}
	}

	@JsonIgnore
	public Optional<SUBACKPayload> getSUBACKPayload() {
		if (this instanceof SUBACKPayload) {
			return Optional.of((SUBACKPayload) this);
		} else {
			return Optional.empty();
		}
	}

	@JsonIgnore
	public Optional<SUBSCRIBEPayload> getSUBSCRIBEPayload() {
		if (this instanceof SUBSCRIBEPayload) {
			return Optional.of((SUBSCRIBEPayload) this);
		} else {
			return Optional.empty();
		}
	}

	/*****************************************************************
	 * JSON and String
	 ****************************************************************/

	@Override
	public String toString() {
		return JSONable.toJSON(this);
	}

}
