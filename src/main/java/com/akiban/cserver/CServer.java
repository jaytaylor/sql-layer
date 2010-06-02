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
import com.akiban.ais.io.Reader;
import com.akiban.ais.message.AISExecutionContext;
import com.akiban.ais.message.AISRequest;
import com.akiban.ais.message.AISResponse;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.AkibaInformationSchemaImpl;
import com.akiban.ais.model.Source;
import com.akiban.cserver.message.ShutdownRequest;
import com.akiban.cserver.message.ShutdownResponse;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.Store;
import com.akiban.message.AkibaConnection;
import com.akiban.message.AkibaConnectionImpl;
import com.akiban.message.ErrorResponse;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Message;
import com.akiban.message.MessageRegistry;
import com.akiban.network.AkibaNetworkHandler;
import com.akiban.network.CommEventNotifier;
import com.akiban.network.NetworkHandlerFactory;
import com.akiban.util.Tap;

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
	private static final String P_CSERVER_PORT = "cserver.port|5140";

	/**
	 * Config property name and default for setting of the verbose flag. When
	 * true, many CServer methods log verbosely at INFO level.
	 */

	private static final String VERBOSE_PROPERTY_NAME = "cserver.verbose";
	
	private static Tap CSERVER_EXEC = Tap.add(new Tap.PerThread("cserver", Tap.TimeStampLog.class));
	
	private final RowDefCache rowDefCache = new RowDefCache();
	private final CServerConfig config = new CServerConfig();
	private final Store hstore = new PersistitStore(config, rowDefCache);
	private AkibaInformationSchema ais0;
	private AkibaInformationSchema ais;
	private volatile boolean stopped = false;
	private boolean verbose;
	private Map<Integer, Thread> threadMap = new TreeMap<Integer, Thread>();
	
	public void start() throws Exception {
		Tap.registerMXBean();
		MessageRegistry.initialize();
		MessageRegistry.only().registerModule("com.akiban.cserver");
		MessageRegistry.only().registerModule("com.akiban.ais");
		MessageRegistry.only().registerModule("com.akiban.message");
		ChannelNotifier callback = new ChannelNotifier();
		NetworkHandlerFactory.initializeNetwork(property(P_CSERVER_HOST),
				property(P_CSERVER_PORT), (CommEventNotifier) callback);
		final String verboseString = config.property(VERBOSE_PROPERTY_NAME
				+ "|false");
		if ("true".equalsIgnoreCase(verboseString)) {
			verbose = true;
		}
		ais0 = primordialAIS();
		rowDefCache.setAIS(ais0);
		
		hstore.startUp();
		hstore.setVerbose(verbose);
		hstore.setOrdinals();
		acquireAIS();
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
		hstore.shutDown();
		NetworkHandlerFactory.closeNetwork();
		Tap.unregisterMXBean();
	}

	public class ChannelNotifier implements CommEventNotifier {

		@Override
		public void onConnect(AkibaNetworkHandler handler) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Connection #" + handler.getId() + " created");
			}
			final String threadName = "CServer_" + handler.getId();
			final Thread thread = new Thread(new CServerRunnable(
					AkibaConnectionImpl.createConnection(handler)), threadName);
			thread.setDaemon(true);
			thread.start();
			synchronized (threadMap) {
				threadMap.put(handler.getId(), thread);
			}
		}

		@Override
		public void onDisconnect(AkibaNetworkHandler handler) {
			final Thread thread;
			synchronized (threadMap) {
				thread = threadMap.remove(handler.getId());
			}
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

	public Store getStore() {
		return hstore;
	}
	
	public class CServerContext implements ExecutionContext,
			AISExecutionContext, CServerShutdownExecutionContext {

	    public Store getStore() {
	        return hstore;
	    }

        public AkibaInformationSchema ais() {
            return ais;
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
					CSERVER_EXEC.in();
					message.execute(connection, context);
					CSERVER_EXEC.out();
					requestCounter++;
				} catch (InterruptedException e) {
					if (LOG.isInfoEnabled()) {
						LOG.info("Thread " + Thread.currentThread().getName()
								+ (stopped ? " stopped" : " interrupted"));
					}
					break;
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
	public synchronized void acquireAIS() throws Exception {
		final Source source = new CServerAisSource(hstore);
		this.ais = new Reader(source)
				.load(new AkibaInformationSchemaImpl(ais0));
		installAIS();
	}

	private synchronized void installAIS() throws Exception {
		if (LOG.isInfoEnabled()) {
			LOG.info("Installing " + ais.getDescription() + " in ChunkServer");
		}
		rowDefCache.clear();
		rowDefCache.setAIS(ais);
		hstore.setOrdinals();
	}

	/**
	 * Loads the built-in primordial table definitions for the
	 * akiba_information_schema tables.
	 * 
	 * @throws Exception
	 */
	public AkibaInformationSchema primordialAIS() throws Exception {
		if (ais0 != null) {
			return ais0;
		}
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
		ais0 = (new DDLSource()).buildAISFromString(sb.toString());
		return ais0;
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

                // HAZEL: MySQL Conference Demo 4/2010: MySQL/Drizzle/Memcache to Chunk Server
                /*
                com.thimbleware.jmemcached.protocol.MemcachedCommandHandler.registerCallback(
                    new com.thimbleware.jmemcached.protocol.MemcachedCommandHandler.Callback()
                    {
                        public byte[] get(byte[] key)
                        {
                            byte[] result = null;

                            String request = new String(key);
                            String[] tokens = request.split(":");
                            if (tokens.length == 4)
                            {
                                String schema = tokens[0];
                                String table = tokens[1];
                                String colkey = tokens[2];
                                String colval = tokens[3];

                                try
                                {
                                    List<RowData> list = null;
                                    //list = server.store.fetchRows(schema, table, colkey, colval, colval, "order");
                                    list = server.store.fetchRows(schema, table, colkey, colval, colval, null);

                                    StringBuilder builder = new StringBuilder();
                                    for (RowData data: list)
                                    {
                                        builder.append(data.toString(server.getRowDefCache()) + "\n");
                                        //builder.append(data.toString());
                                    }

                                    result = builder.toString().getBytes();
                                }
                                catch (Exception e)
                                {
                                    result = new String("read error: " + e.getMessage()).getBytes();
                                }
                            }
                            else
                            {
                                result = new String("invalid key: " + request).getBytes();
                            }

                            return result;
                        }
                    });
                com.thimbleware.jmemcached.Main.main(new String[0]);
                */
	}

	public String property(final String key) {
		return config.property(key);
	}

	public String property(final String key, final String dflt) {
		return config.property(key, dflt);
	}
	
	/**
	 * For unit tests
	 * @param key
	 * @param value
	 */
	public void setProperty(final String key, final String value) {
		config.setProperty(key, value);
	}
}
