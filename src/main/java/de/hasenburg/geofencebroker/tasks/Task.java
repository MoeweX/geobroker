package de.hasenburg.geofencebroker.tasks;

import de.hasenburg.geofencebroker.model.exceptions.TaskException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;

public abstract class Task<V> implements Callable<V> {

	private static final Logger logger = LogManager.getLogger();

	protected TaskManager.TaskName name;
	private TaskManager taskManager;

	protected Task(TaskManager.TaskName name, TaskManager taskManager) {
		this.name = name;
		this.taskManager = taskManager;
	}

	@Override
	public V call() throws TaskException {
		logger.debug("Executing task " + name);
		taskManager.registerTask(name);
		V answer = executeFunctionality();
		taskManager.deregisterTask(name);
		logger.debug("Ending Task " + name + " completed");
		return answer;
	}

	public abstract V executeFunctionality() throws TaskException;

}
