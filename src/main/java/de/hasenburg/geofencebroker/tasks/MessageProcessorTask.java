package de.hasenburg.geofencebroker.tasks;

import de.hasenburg.geofencebroker.communication.RouterCommunicator;
import org.zeromq.ZMsg;
import zmq.socket.reqrep.Router;

import java.util.Random;
import java.util.concurrent.BlockingQueue;

/**
 * A Task that continuously processes messages.
 * 
 * @author jonathanhasenburg
 *
 */
class MessageProcessorTask extends Task<Boolean> {

	BlockingQueue<ZMsg> messageQueue;
	RouterCommunicator routerCommunicator;

	protected MessageProcessorTask(TaskManager taskManager, BlockingQueue<ZMsg> messageQueue, RouterCommunicator routerCommunicator) {
		super(TaskManager.TaskName.MESSAGE_PROCESSOR_TASK, taskManager);
		this.messageQueue = messageQueue;
		this.routerCommunicator = routerCommunicator;
	}

	@Override
	public Boolean executeFunctionality() {

		int queuedMessages = 0;

		while (!Thread.currentThread().isInterrupted()) {
			if (queuedMessages != messageQueue.size()) {
				System.out.println("Number of queued messages: " + messageQueue.size());
				queuedMessages = messageQueue.size();
			}

			Random random = new Random();
			if (random.nextBoolean()) {
				// let's process a message
				if (messageQueue.size() > 0) {
					ZMsg message = messageQueue.remove();
					message.pollLast(); // remove last element
					message.addString("Dealer, this is Router");
					System.out.println(message.toString());

					routerCommunicator.sendMessage(message);
				}
			}

			// sleep 2 seconds
			try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
		}

		return true;
	}

}
