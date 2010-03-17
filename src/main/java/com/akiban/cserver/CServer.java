/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.akiban.cserver;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.io.MySQLSource;
import com.akiban.ais.io.Reader;
import com.akiban.ais.message.AISExecutionContext;
import com.akiban.ais.message.AISRequest;
import com.akiban.ais.message.AISResponse;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.message.ShutdownRequest;
import com.akiban.cserver.message.ShutdownResponse;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.Store;
import com.akiban.message.AkibaConnection;
import com.akiban.message.ErrorResponse;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Message;
import com.akiban.message.MessageRegistry;
import com.akiban.network.AkibaNetworkHandler;
import com.akiban.network.CommEventNotifier;
import com.akiban.network.NetworkHandlerFactory;

/**
 * 
 * @author peter
 */
public class CServer {

	private static final Log LOG = LogFactory.getLog(CServer.class.getName());

	private static final String AIS_DDL_NAME = "akiba_information_schema.ddl";

	/**
	 * Config property name and default for the port on which the CServer will
	 * listen for requests.
	 */
	private static final String P_CSERVER_HOST = "cserver.host|localhost";

	/**
	 * Config property name and default for the port on which the CServer will
	 * listen for requests.
	 */
	private static final String P_CSERVER_PORT = "cserver.port|8080";

	/**
	 * Config property name and default for the MySQL server host from which
	 * CServer will obtain the AIS.
	 */
	private static final String P_AISHOST = "mysql.host|localhost";

	/**
	 * Config property port and default for the MySQL server host from which
	 * Cserver will obtain the AIS.
	 */
	private static final String P_AISPORT = "mysql.port|3306";

	/**
	 * Config property name and default for the MySQL server host from which
	 * CServer will obtain the AIS.
	 */
	private static final String P_AISUSER = "mysql.username|akiba";

	/**
	 * Config property name and default for the MySQL server host from which
	 * CServer will obtain the AIS.
	 */
	private static final String P_AISPASSWORD = "mysql.password|akibaDB";

	private final RowDefCache rowDefCache = new RowDefCache();

	private final CServerConfig config = new CServerConfig();

	private final Store store = new PersistitStore(config, rowDefCache);

	private AkibaInformationSchema ais0;

	private AkibaInformationSchema ais;

	private volatile boolean stopped = false;

	private Map<Integer, Thread> threadMap = new TreeMap<Integer, Thread>();

	public void start() throws Exception {

		MessageRegistry.initialize();
		MessageRegistry.only().registerModule("com.akiban.cserver");
		MessageRegistry.only().registerModule("com.akiban.ais");
		MessageRegistry.only().registerModule("com.akiban.message");
		ChannelNotifier callback = new ChannelNotifier();
		NetworkHandlerFactory.initializeNetwork(property(P_CSERVER_HOST),
				property(P_CSERVER_PORT), (CommEventNotifier) callback);
		loadAis0();
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
			AISExecutionContext, CServerShutdownExecutionContext {

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

		@Override
		public void executeRequest(AkibaConnection connection,
				ShutdownRequest request) throws Exception {
			if (LOG.isInfoEnabled()) {
				LOG.info("CServer stopping due to ShutdownRequest");
			}
			stop();
			ShutdownResponse response = new ShutdownResponse();
			connection.send(response);
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
			while (!stopped) {
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
							LOG.error("Caught " + f.getClass()
									+ " while sending error response to "
									+ message + ": " + f.getMessage(), f);
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
		readAISFromMySQL();
		installAIS();
	}

	private void readAISFromMySQL() throws Exception {

		for (;;) {
			try {
				if (LOG.isInfoEnabled()) {
					LOG.info(String.format("Attempting to load AIS from %s",
							property(P_AISHOST)));
				}
				ais = new Reader(new MySQLSource(property(P_AISHOST),
						property(P_AISPORT), property(P_AISUSER),
						property(P_AISPASSWORD))).load();
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
		rowDefCache.clear();
		rowDefCache.setAIS(ais0);
		rowDefCache.setAIS(ais);
	}

	/**
	 * Loads the built-in primordial table definitions for the akiba_information_schema
	 * tables.
	 * @throws Exception
	 */
	private void loadAis0() throws Exception {
		final DataInputStream stream = new DataInputStream(getClass()
				.getClassLoader().getResourceAsStream(AIS_DDL_NAME));
		// TODO: ugly, but gets the job done
		final StringBuilder sb = new StringBuilder();
		for (;;) {
			final String line = stream.readLine();
			if (line == null) {
				break;
			}
			sb.append(line);
			sb.append("\n");
		}
		this.ais0 = (new DDLSource()).buildAISFromString(sb.toString());
		rowDefCache.setAIS(ais0);
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception {
		final CServer server = new CServer();
		server.config.load();
		if (server.config.getException() != null) {
			LOG.fatal("CServer configuration failed");
			return;
		}
		server.start();
	}

	public String property(final String key) {
		return config.property(key);
	}

	public String property(final String key, final String dflt) {
		return config.property(key, dflt);
	}
}
