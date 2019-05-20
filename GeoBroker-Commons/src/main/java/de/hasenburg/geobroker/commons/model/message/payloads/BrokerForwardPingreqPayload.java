package de.hasenburg.geobroker.commons.model.message.payloads;

import java.util.Objects;

@SuppressWarnings("WeakerAccess") // as fields are checked by AbstractPayload.nullField()
public class BrokerForwardPingreqPayload extends AbstractPayload {

	String clientIdentifier;
	PINGREQPayload pingreqPayload;

	public BrokerForwardPingreqPayload() {

	}

	public BrokerForwardPingreqPayload(String clientIdentifier, PINGREQPayload pingreqPayload) {
		super();
		this.clientIdentifier = clientIdentifier;
		this.pingreqPayload = pingreqPayload;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/

	public String getClientIdentifier() {
		return clientIdentifier;
	}

	public PINGREQPayload getPingreqPayload() {
		return pingreqPayload;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BrokerForwardPingreqPayload that = (BrokerForwardPingreqPayload) o;
		return Objects.equals(clientIdentifier, that.clientIdentifier) && Objects.equals(pingreqPayload,
				that.pingreqPayload);
	}

	@Override
	public int hashCode() {
		return Objects.hash(clientIdentifier, pingreqPayload);
	}
}
