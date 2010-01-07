/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.akiba.cserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import com.akiba.cserver.store.PersistitStore;
import com.akiba.cserver.store.Store;
import com.akiba.message.AkibaConnection;
import com.akiba.message.Message;
import com.akiba.message.MessageRegistry;
import com.akiba.network.AkibaNetworkHandler;
import com.akiba.network.CommEventNotifier;
import com.akiba.network.NetworkHandlerFactory;

/**
 * 
 * @author pbeaman
 */
public class CServer {

	private static final Logger LOGGER = Logger.getLogger(CServer.class
			.getName());

	private final Store store = new PersistitStore();

	private volatile boolean stopped = false;

	private List<Thread> threads = new ArrayList<Thread>();

	public void start() throws Exception {
		MessageRegistry.initialize();
		ChannelNotifier callback = new ChannelNotifier();
		NetworkHandlerFactory.initializeNetwork("localhost", "8080",
				(CommEventNotifier) callback);
		// store.startUp();
	}

	public void stop() throws Exception {
		stopped = true;
		final List<Thread> copy;
		synchronized (threads) {
			copy = new ArrayList<Thread>(threads);
		}
		// fpor now I think this is the only way to make these threads
		// bail from their reads.
		for (final Thread thread : copy) {
			thread.interrupt();
		}
		// store.shutDown();
		NetworkHandlerFactory.closeNetwork();
	}

	public class ChannelNotifier implements CommEventNotifier {

		private volatile int connectCounter = 0;

		@Override
		public void onConnect(AkibaNetworkHandler handler) {
			int counter = ++connectCounter;
			System.out.println("Connection #" + connectCounter + " created");
			LOGGER.info("Connection #" + connectCounter + " created");
			final Thread thread = new Thread(new CServerRunnable(
					AkibaConnection.createConnection(handler)), "CServer_"
					+ counter);
			thread.setDaemon(true);
			thread.start();
			synchronized (threads) {
				threads.add(thread);
			}
		}

		@Override
		public void onDisconnect(AkibaNetworkHandler handler) {
			// TODO
		}
	}

	/**
	 * A Runnable that reads Network messages, acts on them and returns results.
	 * 
	 * @author peter
	 * 
	 */
	private class CServerRunnable implements Runnable {

		private final AkibaConnection connection;

		private int requestCounter;

		public CServerRunnable(final AkibaConnection connection) {
			this.connection = connection;
		}

		public void run() {

			while (!stopped) { // TODO - shutdown
				try {
					Message message = connection.receive();
					message.execute(connection);
					requestCounter++;
				} catch (InterruptedException e) {
					System.err.println("Thread " + Thread.currentThread()
							+ (stopped ? " stopped" : " interrupted"));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception {
		final CServer server = new CServer();
		server.start();
		try {
			//
			// For now this is "crash-only software" - there is no
			// graceful shutdown. Just kill or ctrl-c the process.
			// TODO - needs to change soon - we normally want to complete
			// Persistit shutdown.
			//
			while (true) {
				Thread.sleep(5000);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		server.stop();
	}

}
