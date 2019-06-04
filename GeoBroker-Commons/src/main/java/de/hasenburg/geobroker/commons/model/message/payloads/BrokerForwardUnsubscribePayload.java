package de.hasenburg.geobroker.commons.model.message.payloads;

import java.util.Objects;

@SuppressWarnings("WeakerAccess") // as fields are checked by AbstractPayload.nullField()
public class BrokerForwardUnsubscribePayload extends AbstractPayload {

	String clientIdentifier;
	UNSUBSCRIBEPayload unsubscribePayload;

	public BrokerForwardUnsubscribePayload(String clientIdentifier, UNSUBSCRIBEPayload unsubscribePayload) {
		super();
		this.clientIdentifier = clientIdentifier;
		this.unsubscribePayload = unsubscribePayload;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/


	public String getClientIdentifier() {
		return clientIdentifier;
	}

	public UNSUBSCRIBEPayload getUnsubscribePayload() {
		return unsubscribePayload;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BrokerForwardUnsubscribePayload that = (BrokerForwardUnsubscribePayload) o;
		return Objects.equals(getClientIdentifier(), that.getClientIdentifier()) &&
				Objects.equals(getUnsubscribePayload(), that.getUnsubscribePayload());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getClientIdentifier(), getUnsubscribePayload());
	}
}
