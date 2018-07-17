package de.hasenburg.geofencebroker.tasks;

/**
 * A task that sleeps a given amount of time.
 * 
 * @author jonathanhasenburg
 *
 */
class SleepTask extends Task<Boolean> {

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
			System.out.println("Sleep time must be greater than 0, but is " + time + ".");
		}
		return true;
	}

}
