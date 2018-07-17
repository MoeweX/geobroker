package de.hasenburg.geofencebroker.tasks;

import org.zeromq.ZMsg;

import java.util.concurrent.BlockingQueue;

/**
 * A Task that continuously processes messages.
 * 
 * @author jonathanhasenburg
 *
 */
class MessageProcessorTask extends Task<Boolean> {

	BlockingQueue<ZMsg> messageQueue;

	protected MessageProcessorTask(TaskManager taskManager, BlockingQueue<ZMsg> messageQueue) {
		super(TaskManager.TaskName.MESSAGE_PROCESSOR_TASK, taskManager);
		this.messageQueue = messageQueue;
	}

	@Override
	public Boolean executeFunctionality() {

		int queuedMessages = 0;

		while (!Thread.currentThread().isInterrupted()) {
			if (queuedMessages != messageQueue.size()) {
				System.out.println("Number of queued messages: " + messageQueue.size());
				queuedMessages = messageQueue.size();
			}

			// sleep 2 seconds
			try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
		}

		return true;
	}

}
