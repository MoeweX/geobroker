package de.hasenburg.geobroker.commons.communication;

import org.zeromq.ZContext;

public abstract class ZMQProcess implements Runnable {

	protected String identity;

	// Socket and context
	protected ZContext context;

	public ZMQProcess(String identity) {
		this.identity = identity;
	}

	// can't be in constructor as added by ZMQProcessManager
	public void init(ZContext context) {
		this.context = context;
	}

}
