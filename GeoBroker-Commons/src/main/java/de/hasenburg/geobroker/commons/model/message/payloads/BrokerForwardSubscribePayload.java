package de.hasenburg.geobroker.commons.model.message.payloads;

import java.util.Objects;

@SuppressWarnings("WeakerAccess") // as fields are checked by AbstractPayload.nullField()
public class BrokerForwardSubscribePayload extends AbstractPayload {

	String clientIdentifier;
	SUBSCRIBEPayload subscribePayload;

	public BrokerForwardSubscribePayload(String clientIdentifier, SUBSCRIBEPayload subscribePayload) {
		super();
		this.clientIdentifier = clientIdentifier;
		this.subscribePayload = subscribePayload;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/


	public String getClientIdentifier() {
		return clientIdentifier;
	}

	public SUBSCRIBEPayload getSubscribePayload() {
		return subscribePayload;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BrokerForwardSubscribePayload that = (BrokerForwardSubscribePayload) o;
		return Objects.equals(getClientIdentifier(), that.getClientIdentifier()) &&
				Objects.equals(getSubscribePayload(), that.getSubscribePayload());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getClientIdentifier(), getSubscribePayload());
	}
}
