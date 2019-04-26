package de.hasenburg.geobroker.server.main.server;

import de.hasenburg.geobroker.server.main.Configuration;

public class ServerLifecycle {

	private final IServerLogic serverLogic;

	public ServerLifecycle(IServerLogic serverLogic) {
		this.serverLogic = serverLogic;
	}

	public void run(Configuration configuration) {
		serverLogic.loadConfiguration(configuration);

		serverLogic.initializeFields();

		serverLogic.startServer();

		serverLogic.serverIsRunning();

		serverLogic.cleanUp();
	}

}
