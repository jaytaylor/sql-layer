package com.akiban.cserver.util;

import com.akiban.cserver.message.BulkLoadRequest;
import com.akiban.message.AkibaConnection;
import com.akiban.message.MessageRegistry;
import com.akiban.network.AkibaNetworkHandler;
import com.akiban.network.CommEventNotifier;
import com.akiban.network.NetworkHandlerFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulkLoaderClient
{
    public static void main(String[] args) throws Exception
    {
        new BulkLoaderClient(args).run();
    }

    private void run() throws Exception
    {
        startNetwork();
        AkibaNetworkHandler networkHandler =
            NetworkHandlerFactory.getHandler(cserverHost, Integer.toString(cserverPort), null);
        logger.info(String.format("Got network handler %s", networkHandler));
        AkibaConnection connection = AkibaConnection.createConnection(networkHandler);
        logger.info(String.format("Got connection: %s", connection));
        try {
            BulkLoadRequest request = new BulkLoadRequest(dbHost,
                                                          dbPort,
                                                          dbUser,
                                                          dbPassword,
                                                          groups,
                                                          artifactsSchema,
                                                          sourceSchemas,
                                                          resume,
                                                          cleanup);
            logger.info("About to send request");
            connection.send(request);
            logger.info("Request sent");
        } catch (Exception e) {
            logger.error("Caught exception", e);
            throw e;
        } finally {
            logger.info("Closing network");
            NetworkHandlerFactory.closeNetwork();
            logger.info("Network closed");
        }
    }

    private BulkLoaderClient(String[] args) throws Exception
    {
        int a = 0;
        try {
            while (a < args.length) {
                String flag = args[a++];
                if (flag.equals("--resume")) {
                    resume = true;
                } else if (flag.equals("--cleanup")) {
                    cleanup = true;
                } else if (flag.equals("--temp")) {
                    artifactsSchema = args[a++];
                } else if (flag.equals("--mysql")) {
                    String hostAndPort = args[a++];
                    int colon = hostAndPort.indexOf(':');
                    if (colon < 0) {
                        dbHost = hostAndPort;
                        dbPort = DEFAULT_MYSQL_PORT;
                    } else {
                        dbHost = hostAndPort.substring(0, colon);
                        dbPort = Integer.parseInt(hostAndPort.substring(colon + 1));
                    }
                } else if (flag.equals("--user")) {
                    dbUser = args[a++];
                } else if (flag.equals("--password")) {
                    dbPassword = args[a++];
                } else if (flag.equals("--source")) {
                    String targetAndSource = args[a++];
                    int colon = targetAndSource.indexOf(':');
                    if (colon < 0) {
                        usage(null);
                    }
                    String target = targetAndSource.substring(0, colon);
                    String source = targetAndSource.substring(colon + 1);
                    sourceSchemas.put(target, source);
                } else if (flag.equals("--cserver")) {
                    String hostAndPort = args[a++];
                    int colon = hostAndPort.indexOf(':');
                    if (colon < 0) {
                        usage(null);
                    } else {
                        cserverHost = hostAndPort.substring(0, colon);
                        cserverPort = Integer.parseInt(hostAndPort.substring(colon + 1));
                    }
                } else if (flag.equals("--group")) {
                    groups.add(args[a++]);
                } else {
                    usage(null);
                }
            }
        } catch (Exception e) {
            usage(e);
        }
        if (dbHost == null || dbUser == null || groups.isEmpty()) {
            usage(null);
        }
        logger.info(String.format("mysql: %s:%s", dbHost, dbPort));
        logger.info(String.format("user: %s", dbUser));
        logger.info(String.format("password: %s", dbPassword));
        logger.info(String.format("groups: %s", groups));
        logger.info(String.format("cserver: %s:%s", cserverHost, cserverPort));
        logger.info(String.format("temp: %s", artifactsSchema));
        logger.info(String.format("sources: %s", sourceSchemas));
        logger.info(String.format("resume: %s", resume));
        logger.info(String.format("cleanup: %s", cleanup));
    }

    private ChannelNotifier startNetwork()
    {
        MessageRegistry.reset();
        MessageRegistry.initialize();
        MessageRegistry.only().registerModule("com.akiban.cserver.message");
        MessageRegistry.only().registerModule("com.akiban.message");
        ChannelNotifier notifier = new ChannelNotifier();
        NetworkHandlerFactory.initializeNetwork(LOCALHOST,
                                                Integer.toString(BULK_LOADER_CLIENT_LISTENER_PORT),
                                                notifier);
        logger.info("Network started");
        return notifier;
    }

    private static void usage(Exception e) throws Exception
    {
        for (String line : USAGE) {
            System.err.println(line);
        }
        if (null != e) {
            System.err.println(e.getMessage());
        }
        System.exit(1);
    }

    private static final String[] USAGE = {
        "aload --mysql MYSQL_HOST[:MYSQL_PORT] --user USER [--password PASSWORD] (--group GROUP)+ " +
        "--cserver CSERVER_HOST:CSERVER_PORT " +
        "--temp TEMP_SCHEMA (--source TARGET_SCHEMA:SOURCE_SCHEMA)* [--resume] [--nocleanup]",
        "",
        "Copies data from a MySQL database into a chunkserver. Data is transformed in the MySQL database, before it ",
        "is written to the chunkserver. ",
        "",
        "The database being copied is located at MYSQL_HOST:MYSQL_PORT, and is accessed as USER, identified by ",
        "PASSWORD if provided. Only the named GROUPs will be copied. At least one GROUP must be specified, and ",
        "multiple GROUPs may be specified, (repeating the --group flag for each).",
        "",
        "The chunkserver being loaded is located at CSERVER_HOST:CSERVER_PORT.",
        "The database being loaded must already have a schema installed. Loading will add data to presumably empty tables.",
        "By default, the source and target schema names are assumed to match. If they don't, then source schema names",
        "can be specified using one or more --source specifications. For example --source abc:def will copy data from",
        "the abc schema of the database at HOST:PORT into the def schema locally. Table names must match in source and",
        "target schemas.",
        "",
        "If --resume is specified, then the previously attempted migration, whose state is stored in TEMP_SCHEMA, ",
        "is resumed. If --resume is not specified, then any state from a previous load, saved in TEMP_SCHEMA, is lost.",
        "",
        "If --nocleanup is specified, then the TEMP_SCHEMA is not deleted when the load completes.",
        ""
    };

    private static final Logger logger = Logger.getLogger(BulkLoaderClient.class);
    private static final String LOCALHOST = "localhost";
    private static final int DEFAULT_MYSQL_PORT = 3306;
    private static final int BULK_LOADER_CLIENT_LISTENER_PORT = 9999; // because there has to be one

    private String cserverHost;
    private int cserverPort;
    private boolean resume = false;
    private boolean cleanup = false;
    private String artifactsSchema;
    private String dbHost;
    private int dbPort;
    private String dbUser;
    private String dbPassword;
    private final Map<String, String> sourceSchemas = new HashMap<String, String>();
    private final List<String> groups = new ArrayList<String>();

    // Inner classes

    public class ChannelNotifier implements CommEventNotifier
    {
        @Override
        public void onConnect(AkibaNetworkHandler handler)
        {
        }

        @Override
        public void onDisconnect(AkibaNetworkHandler handler)
        {
            handler.disconnectWorker();
        }
    }
}
