/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.akiba.cserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.akiba.message.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiba.ais.io.MySQLSource;
import com.akiba.ais.message.AISExecutionContext;
import com.akiba.ais.message.AISRequest;
import com.akiba.ais.message.AISResponse;
import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.ais.model.AkibaInformationSchemaImpl;
import com.akiba.cserver.store.PersistitStore;
import com.akiba.cserver.store.Store;
import com.akiba.network.AkibaNetworkHandler;
import com.akiba.network.CommEventNotifier;
import com.akiba.network.NetworkHandlerFactory;

/**
 * 
 * @author pbeaman
 */
public class CServer {

	private static final Log LOG = LogFactory.getLog(CServer.class.getName());

	private static final String[] USAGE = {
			"java -jar cserver.jar DB_HOST DB_USERNAME DB_PASSWORD",
			"    DB_HOST: Host running database containing AIS",
			"    DB_USERNAME: Database username",
			"    DB_PASSWORD: Database password", };

	private final RowDefCache rowDefCache = new RowDefCache();

	private final Store store = new PersistitStore(rowDefCache);

	private AkibaInformationSchema ais;

	private String dbHost = "localhost";
	private String dbUsername = "akiba";
	private String dbPassword = "akibaDB";

	private volatile boolean stopped = false;

	private Map<Integer, Thread> threadMap = new TreeMap<Integer, Thread>();

	public void start() throws Exception {
		MessageRegistry.initialize();
		MessageRegistry.only().registerModule("com.akiba.cserver");
		MessageRegistry.only().registerModule("com.akiba.ais");
		MessageRegistry.only().registerModule("com.akiba.message");
		ChannelNotifier callback = new ChannelNotifier();
		NetworkHandlerFactory.initializeNetwork("localhost", "8080",
				(CommEventNotifier) callback);
		store.startUp();
	}

	public void stop() throws Exception {
		stopped = true;
		final List<Thread> copy;
		synchronized (threadMap) {
			copy = new ArrayList<Thread>(threadMap.values());
		}
		// for now I think this is the only way to make these threads
		// bail from their reads.
		for (final Thread thread : copy) {
			thread.interrupt();
		}
		store.shutDown();
		NetworkHandlerFactory.closeNetwork();
	}

	public class ChannelNotifier implements CommEventNotifier {

		@Override
		public void onConnect(AkibaNetworkHandler handler) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Connection #" + handler.getId() + " created");
			}
			final String threadName = "CServer_" + handler.getId();
			final Thread thread = new Thread(new CServerRunnable(
					AkibaConnection.createConnection(handler)), threadName);
			thread.setDaemon(true);
			thread.start();
			synchronized (threadMap) {
				threadMap.put(handler.getId(), thread);
			}
		}

		@Override
		public void onDisconnect(AkibaNetworkHandler handler) {
			final Thread thread = threadMap.remove(handler.getId());
			if (thread != null && thread.isAlive()) {
				thread.interrupt();
				if (LOG.isInfoEnabled()) {
					LOG.info("Connection #" + handler.getId() + " ended");
				}
			} else {
				LOG.error("CServer thread for connection #" + handler.getId()
						+ " was missing or dead");
			}
		}
	}

	public class CServerContext implements ExecutionContext,
			AISExecutionContext {

		public Store getStore() {
			return store;
		}

		@Override
		public void executeRequest(AkibaConnection connection,
				AISRequest request) throws Exception {
			acquireAIS();
			AISResponse aisResponse = new AISResponse(ais);
			connection.send(aisResponse);
		}

		@Override
		public void executeResponse(AkibaConnection connection,
				AISResponse response) throws Exception {
			ais = response.ais();
			installAIS();
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

		private final ExecutionContext context = new CServerContext();

		private int requestCounter;

		public CServerRunnable(final AkibaConnection connection) {
			this.connection = connection;
		}

		public void run() {

            Message message = null;
			while (!stopped) { // TODO - shutdown
				try {
					message = connection.receive();
					if (LOG.isTraceEnabled()) {
						LOG.trace("Serving message " + message);
					}
					message.execute(connection, context);
					requestCounter++;
				} catch (InterruptedException e) {
					if (LOG.isInfoEnabled()) {
						LOG.info("Thread " + Thread.currentThread().getName()
								+ (stopped ? " stopped" : " interrupted"));
					}
				} catch (Exception e) {
					if (LOG.isErrorEnabled()) {
						LOG.error("Unexpected error on " + message, e);
					}
                    if (message != null) {
                        try {
                            connection.send(new ErrorResponse(e));
                        } catch (Exception f) {
                            LOG.error("Caught "
                                      + f.getClass()
                                      + " while sending error response to "
                                      + message
                                      + ": "
                                      + f.getMessage(),
                                      f);
                        }
                    }
				}
			}
		}
	}

	public RowDefCache getRowDefCache() {
		return rowDefCache;
	}

	/**
	 * Acquire an AkibaInformationSchema from MySQL and install it into the
	 * local RowDefCache.
	 * 
	 * This method always refreshes the locally cached AkibaInformationSchema to
	 * support schema modifications at the MySQL head.
	 * 
	 * @return an AkibaInformationSchema
	 * @throws Exception
	 */
	public void acquireAIS() throws Exception {
		LOG.info("Reading AIS from " + dbHost);
		readAISFromMySQL();
		installAIS();
	}

	private void readAISFromMySQL() throws Exception {

		for (;;) {
			try {
				if (LOG.isInfoEnabled()) {
					LOG.info(String.format("Attempting to load AIS from %s",
							dbHost));
				}
				ais = AkibaInformationSchemaImpl.load(new MySQLSource(dbHost,
						dbUsername, dbPassword));
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
		LOG.info("Installing AIS in ChunkServer");
		rowDefCache.setAIS(ais);
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception {
		final CServer server = new CServer();
		server.readArgs(args);
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
