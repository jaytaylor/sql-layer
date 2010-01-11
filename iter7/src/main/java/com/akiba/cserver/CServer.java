/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.akiba.cserver;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.akiba.ais.io.MySQLSource;
import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.ais.model.AkibaInformationSchemaImpl;
import com.akiba.cserver.store.PersistitStore;
import com.akiba.cserver.store.Store;
import com.akiba.message.AkibaConnection;
import com.akiba.message.ExecutionContext;
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

	private static final String[] USAGE = {
			"java -jar cserver.jar DB_HOST DB_USERNAME DB_PASSWORD DB_NAME NET_HOST NET_PORT",
			"    DB_HOST: Host running database containing AIS",
			"    DB_USERNAME: Database username",
			"    DB_PASSWORD: Database password",
			"    NET_HOST: Host to receive AIS",
			"    NET_POST: Port to receive AIS" };

	private final RowDefCache rowDefCache = new RowDefCache();

	private final Store store = new PersistitStore(rowDefCache);

	private volatile boolean stopped = false;

	private List<Thread> threads = new ArrayList<Thread>();

	public void start() throws Exception {
		MessageRegistry.initialize().registerModule("com.akiba.cserver");
		ChannelNotifier callback = new ChannelNotifier();
		NetworkHandlerFactory.initializeNetwork("localhost", "8080",
				(CommEventNotifier) callback);
		store.startUp();
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
		store.shutDown();
		NetworkHandlerFactory.closeNetwork();
	}

	public class ChannelNotifier implements CommEventNotifier {

		private volatile int connectCounter = 0;

		@Override
		public void onConnect(AkibaNetworkHandler handler) {
			int counter = ++connectCounter;
			System.out.println("Connection #" + connectCounter + " created");
			if (LOGGER.isLoggable(Level.INFO)) {
				LOGGER.info("Connection #" + connectCounter + " created");
			}
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

	public static class CServerContext implements ExecutionContext {
		private final CServer cserver;

		public Store getStore() {
			return cserver.store;
		}

		private CServerContext(final CServer cserver) {
			this.cserver = cserver;
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

		private final ExecutionContext context = new CServerContext(CServer.this);

		private int requestCounter;

		public CServerRunnable(final AkibaConnection connection) {
			this.connection = connection;
		}

		public void run() {

			while (!stopped) { // TODO - shutdown
				try {
					Message message = connection.receive();
					if (LOGGER.isLoggable(Level.INFO)) {
						LOGGER.info("Serving message " + message);
					}
					message.execute(connection, context);
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

	public RowDefCache getRowDefCache() {
		return rowDefCache;
	}

	private void usage() {
		for (String line : USAGE) {
			System.err.println(line);
		}
		System.exit(1);
	}

	private void readAISFromMySQL(final String[] args) throws Exception {
		int a = 0;
		final String dbHost;
		final String dbUsername;
		final String dbPassword;
		final String dbName;
		try {
			dbHost = args[a++];
			dbUsername = args[a++];
			dbPassword = args[a++];
			dbName = args[a++];
		} catch (Exception e) {
			usage();
			throw e;
		}
		for (;;) {
			try {
				if (LOGGER.isLoggable(Level.INFO)) {
					LOGGER.info(String
							.format("Attempting to load AIS from %s:%s",
									dbHost, dbName));
				}
				final AkibaInformationSchema ais = AkibaInformationSchemaImpl
						.load(new MySQLSource(dbHost, dbUsername, dbPassword,
								dbName));
				rowDefCache.setAIS(ais);
				break;
			} catch (com.mysql.jdbc.exceptions.jdbc4.CommunicationsException e) {
				try {
					Thread.sleep(30000L);
				} catch (InterruptedException ie) {
					break;
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
		if (args.length > 0) {
			server.readAISFromMySQL(args);
		}
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
