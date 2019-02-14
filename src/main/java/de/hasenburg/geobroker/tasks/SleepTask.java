package de.hasenburg.geobroker.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A task that sleeps a given amount of time.
 * 
 * @author jonathanhasenburg
 *
 */
class SleepTask extends Task<Boolean> {

	private static final Logger logger = LogManager.getLogger();
	private int time;

	protected SleepTask(TaskManager taskManager, int time) {
		super(TaskManager.TaskName.SLEEP, taskManager);
		this.time = time;
	}

	@Override
	public Boolean executeFunctionality() {
		if (time > 0) {
			try {
				Thread.sleep(time);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return false;
			}
		} else {
			logger.error("Sleep time must be greater than 0, but is " + time + ".");
		}
		return true;
	}

}
