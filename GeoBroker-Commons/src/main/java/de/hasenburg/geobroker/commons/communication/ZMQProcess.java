package de.hasenburg.geobroker.commons.communication;

import org.zeromq.ZContext;

public abstract class ZMQProcess implements Runnable {

	//TODO add identity and backend string as field here

	// Socket and context
	protected ZContext context;

	public void init(ZContext context) {
		this.context = context;
	}

}
