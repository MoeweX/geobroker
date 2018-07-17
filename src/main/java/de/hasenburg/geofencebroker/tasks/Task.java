package de.hasenburg.geofencebroker.tasks;

import de.hasenburg.geofencebroker.exceptions.TaskException;

import java.util.concurrent.Callable;

public abstract class Task<V> implements Callable<V> {

	protected TaskManager.TaskName name;
	private TaskManager taskManager;

	protected Task(TaskManager.TaskName name, TaskManager taskManager) {
		this.name = name;
		this.taskManager = taskManager;
	}

	@Override
	public V call() throws TaskException {
		System.out.println("Executing task " + name);
		taskManager.registerTask(name);
		V answer = executeFunctionality();
		taskManager.deregisterTask(name);
		System.out.println("Ending Task " + name + " completed");
		return answer;
	}

	public abstract V executeFunctionality() throws TaskException;

}
