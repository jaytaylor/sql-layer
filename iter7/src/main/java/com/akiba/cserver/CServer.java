/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.akiba.cserver;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.akiba.ais.io.MySQLSource;
import com.akiba.ais.message.AISResponse;
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
			"java -jar cserver.jar DB_HOST DB_USERNAME DB_PASSWORD DB_NAME",
			"    DB_HOST: Host running database containing AIS",
			"    DB_USERNAME: Database username",
			"    DB_PASSWORD: Database password",
			"    DB_NAME: AIS dababase name",
			"    NET_HOST: ASE host name (usually same as DB_HOST)",
			"    NET_PORT: ASE port address," };

	private final RowDefCache rowDefCache = new RowDefCache();

	private final Store store = new PersistitStore(rowDefCache);

	private AkibaInformationSchema ais;

	private String dbHost = "localhost";
	private String dbUsername = "akiba";
	private String dbPassword = "akibaDB";
	private String dbName = "akiba_information_schema";
	private String netHost = "localhost";
	private String netPort = "33060";
	private String toFile = null;
	
	private volatile boolean stopped = false;

	private List<Thread> threads = new ArrayList<Thread>();

	public void start() throws Exception {
		MessageRegistry.only().registerModule("com.akiba.cserver");
		MessageRegistry.only().registerModule("com.akiba.ais");
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
		private final Store store;

		public Store getStore() {
			return store;
		}

		private CServerContext(final Store store) {
			this.store = store;
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

		private final ExecutionContext context = new CServerContext(store);

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

	private void setUpAIS(final String[] args) throws Exception {
		if (args.length > 0) {
			readArgs(args);
			readAISFromMySQL();
			LOGGER.info("Acquired AIS from " + dbHost + ":" + dbName);
			installAIS();
			LOGGER.info("Install AIS in ChunkServer");
			sendAISToNetwork();
			LOGGER.info("Sent AIS to " + netHost + ":" + netPort);
			if (toFile != null) {
				final ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(toFile));
				oos.writeObject(ais);
				oos.close();
				LOGGER.info("Wrote AIS to file " + toFile);
			}
		}
	}


	private void readAISFromMySQL() throws Exception {

		for (;;) {
			try {
				if (LOGGER.isLoggable(Level.INFO)) {
					LOGGER.info(String
							.format("Attempting to load AIS from %s:%s",
									dbHost, dbName));
				}
				ais = AkibaInformationSchemaImpl.load(new MySQLSource(dbHost,
						dbUsername, dbPassword, dbName));
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

	private void installAIS() {
		rowDefCache.setAIS(ais);
	}

	private void sendAISToNetwork() throws Exception {
		AkibaConnection connection = AkibaConnection
				.createConnection(NetworkHandlerFactory.getHandler(netHost,
						netPort, null));
		AISResponse aisResponse = new AISResponse(ais);
		connection.send(aisResponse);
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception {
		final CServer server = new CServer();
		MessageRegistry.initialize();
		server.start();
		server.setUpAIS(args);
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

	private void readArgs(final String[] args) {
		int a = 0;
		if (a < args.length) {
			dbHost = args[a++];
		}
		if (a < args.length) {
			dbUsername = args[a++];
		}
		if (a < args.length) {
			dbPassword = args[a++];
		}
		if (a < args.length) {
			dbName = args[a++];
		}
		if (a < args.length) {
			netHost = args[a++];
		}
		if (a < args.length) {
			netPort = args[a++];
		}
		if (a < args.length) {
			toFile = args[a++];
		}
		if (dbHost.contains("-h") || dbHost.contains("?")) {
			usage();
		}
	}

	private void usage() {
		for (String line : USAGE) {
			System.err.println(line);
		}
		System.exit(1);
	}

}
