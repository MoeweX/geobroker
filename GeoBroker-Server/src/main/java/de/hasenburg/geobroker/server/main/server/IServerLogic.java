package de.hasenburg.geobroker.server.main.server;

import de.hasenburg.geobroker.server.main.Configuration;

public interface IServerLogic {

	void loadConfiguration(Configuration configuration);

	void initializeFields();

	void startServer();

	void serverIsRunning();

	void cleanUp();

}
