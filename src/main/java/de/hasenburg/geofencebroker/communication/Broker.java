package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.model.exceptions.CommunicatorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

public class Broker {

	private static final Logger logger = LogManager.getLogger();

	// Address and port of broker
	private String address;
	private int port;

	// Thread management
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private Future<?> runnableFuture;

	// Socket and context
	private ZContext context;
	private ZMQ.Socket frontend;
	private ZMQ.Socket backend;

	public Broker(String address, int port) {
		this.address = address;
		this.port = port;
	}

	public void init() throws CommunicatorException {
		if (executor.isShutdown()) {
			throw new CommunicatorException("Broker shutdown, cannot be reused.");
		}

		if (isBrokering()) {
			throw new CommunicatorException("Broker already brokering");
		}

		context = new ZContext();
		frontend = context.createSocket(ZMQ.ROUTER);
		frontend.bind(address + ":" + port);

		backend = context.createSocket(ZMQ.DEALER);
		backend.bind("inproc://backend");

		runnableFuture = executor.submit(() -> {
			ZMQ.proxy(frontend, backend, null);
			context.destroySocket(frontend);
			logger.debug("Frontend socket destroyed");
			context.destroySocket(backend);
			logger.debug("Backend socket destroyed");
			logger.info("Broker stopped to broker as context is beeing destroyed");
		});

		logger.info("Broker ready to broker.");
	}

	public ZContext getContext() {
		return context;
	}

	public boolean isBrokering() {
		return runnableFuture != null && !runnableFuture.isDone();
	}

	public void tearDown() {
		if (this.runnableFuture != null) {
			logger.info("Trying to destroy broker context");
			context.destroy();
			logger.info("Destroyed Broker context");
			executor.shutdown();
		}

		this.executor.shutdownNow();
		logger.info("Tear down of broker completed, communicator cannot be reused." + (executor.isTerminated() ? "" : " Some tasks may still be running."));
	}

	public static void main (String[] args) throws CommunicatorException {
		for (int i = 0; i < 10; i++) {
			Broker broker = new Broker("tcp://localhost", 5559);
			broker.init();
			logger.info("Broker is brokering?: {}", broker.isBrokering());
			broker.tearDown();
			logger.info("\n\n");
		}
	}

}
