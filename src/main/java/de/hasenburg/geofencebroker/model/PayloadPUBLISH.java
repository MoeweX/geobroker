package de.hasenburg.geofencebroker.model;

import java.util.Objects;
import java.util.Optional;

public class PayloadPUBLISH extends Payload {

	private String content;

	public PayloadPUBLISH() {

	}

	public PayloadPUBLISH(String content) {
		this.content = content;
	}

	public Optional<String> getContent() {
		return Optional.ofNullable(content);
	}

	/*****************************************************************
	 * Test Main
	 ****************************************************************/

	public static void main (String[] args) {
		System.out.println(JSONable.toJSON(new PayloadPUBLISH("Content")));
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof PayloadPUBLISH)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		PayloadPUBLISH that = (PayloadPUBLISH) o;
		return Objects.equals(getContent(), that.getContent());
	}

	@Override
	public int hashCode() {

		return Objects.hash(super.hashCode(), getContent());
	}
}
