/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.loader.Event;
import com.akiban.server.message.BulkLoadRequest;
import com.akiban.server.message.BulkLoadResponse;
import com.akiban.server.message.BulkLoadStatusRequest;
import com.akiban.message.AkibanConnection;
import com.akiban.message.AkibanConnectionImpl;
import com.akiban.message.ErrorResponse;
import com.akiban.message.Request;
import com.akiban.message.Response;

public class BulkLoaderClient
{
    public static void main(String[] args) 
    {
	    try {
	        new BulkLoaderClient(args).run();
	    } catch (Exception e) {
            //die with non-zero return code
            //java prog does not return correctly when main method throws
            e.printStackTrace();
            System.exit(1);

        }
    }

    private void run() throws Exception
    {
        connection = new AkibanConnectionImpl(akserverHost, akserverPort);
        logger.info(String.format("Got connection: %s", connection));
        int exitCode = 0;
        try {
            Request request;
            if (monitor) {
                request = new BulkLoadStatusRequest(lastEventId);
            } else if (resume) {
                request = BulkLoadRequest.resume(dbHost,
                                                 dbPort,
                                                 dbUser,
                                                 dbPassword,
                                                 groups,
                                                 artifactsSchema,
                                                 sourceSchemas,
                                                 cleanup);
            } else {
                request = BulkLoadRequest.start(dbHost,
                                                dbPort,
                                                dbUser,
                                                dbPassword,
                                                groups,
                                                artifactsSchema,
                                                sourceSchemas,
                                                cleanup);
            }
            BulkLoadResponse response;
            BulkLoadResponse badEnding = null;
            do {
                response = runRequest(request);
                if (response.terminatedByException()) {
                    badEnding = response;
                } else {
                    if (!response.isIdle()) {
                        Thread.sleep(TIME_BETWEEN_REQUESTS_MSEC);
                        request = new BulkLoadStatusRequest(lastEventId);
                    }
                }
            } while (badEnding == null && !response.isIdle());
            if (!monitor) {
                if (badEnding == null) {
                    request = BulkLoadRequest.done(dbHost,
                                                   dbPort,
                                                   dbUser,
                                                   dbPassword,
                                                   groups,
                                                   artifactsSchema,
                                                   sourceSchemas,
                                                   cleanup);
                    runRequest(request);
                } else {
                    logger.error(String.format("Bulk load terminated by %s: %s",
                                               response.exceptionClassName(), response.exceptionMessage()));
                    exitCode = response.exitCode();
                }
            }
        } catch (Exception e) {
            logger.error("Caught exception", e);
            throw e;
        } finally {
            logger.info("Closing akserver connection");
            connection.close();
            logger.info(String.format("Exit code: %s", exitCode));
            System.exit(exitCode);
        }
    }

    private BulkLoadResponse runRequest(Request request) throws Exception
    {
        logger.info(String.format("About to send request %s", request));
        Response response = (Response) connection.sendAndReceive(request);
        logger.info(String.format("Received response %s", response));
        if (response instanceof ErrorResponse) {
            ErrorResponse errorResponse = (ErrorResponse) response;
            logger.error(errorResponse.message());
            logger.error(errorResponse.remoteStack());
            throw new RuntimeException(errorResponse.message());
        } else {
            BulkLoadResponse bulkLoadResponse = (BulkLoadResponse) response;
            List<Event> events = bulkLoadResponse.events();
            Event lastEvent = null;
            if (events != null) {
                for (Event event : events) {
                    logger.info(String.format("%s (%s sec): %s", event.eventId(), event.timeSec(), event.message()));
                    lastEvent = event;
                }
                if (lastEvent != null) {
                    lastEventId = lastEvent.eventId();
                }
            }
            return bulkLoadResponse;
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
                } else if (flag.equals("--monitor")) {
                    monitor = true;
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
                        dbPort = Integer.parseInt(hostAndPort
                                .substring(colon + 1));
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
                } else if (flag.equals("--akserver")) {
                    String hostAndPort = args[a++];
                    int colon = hostAndPort.indexOf(':');
                    if (colon < 0) {
                        usage(null);
                    } else {
                        akserverHost = hostAndPort.substring(0, colon);
                        akserverPort = Integer.parseInt(hostAndPort
                                .substring(colon + 1));
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
        if (akserverHost == null) {
            usage(null);
        }
        if (!monitor && (dbHost == null || dbUser == null || groups.isEmpty())) {
            usage(null);
        }
        logger.info(String.format("akserver: %s:%s", akserverHost, akserverPort));
        logger.info(String.format("monitor: %s", monitor));
        if (!monitor) {
            logger.info(String.format("resume: %s", resume));
            logger.info(String.format("cleanup: %s", cleanup));
            logger.info(String.format("mysql: %s:%s", dbHost, dbPort));
            logger.info(String.format("user: %s", dbUser));
            logger.info(String.format("password: %s", dbPassword));
            logger.info(String.format("groups: %s", groups));
            logger.info(String.format("temp: %s", artifactsSchema));
            logger.info(String.format("sources: %s", sourceSchemas));
        }
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
            "aload [--resume] [--nocleanup] --mysql MYSQL_HOST[:MYSQL_PORT] --user USER [--password PASSWORD] (--group GROUP)+ --akserver AKSERVER_HOST:AKSERVER_PORT [--temp TEMP_SCHEMA] (--source TARGET_SCHEMA:SOURCE_SCHEMA)*",
            "aload [--monitor] --akserver AKSERVER_HOST:AKSERVER_PORT",
            "",
            "Copies data from a MySQL database into a chunkserver. Data is transformed in the MySQL database, before it ",
            "is written to the chunkserver. ",
            "",
            "The database being copied is located at MYSQL_HOST:MYSQL_PORT, and is accessed as USER, identified by ",
            "PASSWORD if provided. Only the named GROUPs will be copied. At least one GROUP must be specified, and ",
            "multiple GROUPs may be specified, (repeating the --group flag for each).",
            "",
            "The chunkserver being loaded is located at AKSERVER_HOST:AKSERVER_PORT.",
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
            "",
            "If TEMP_SCHEMA is not specified, then a schema name will be generated.",
            "",
            "If --monitor is specified, then a bulk load is neither started nor resumed. Instead, an ongoing bulk load",
            "is monitored.",
            ""};

    private static final Logger logger = LoggerFactory.getLogger(BulkLoaderClient.class);
    private static final String LOCALHOST = "localhost";
    private static final int DEFAULT_MYSQL_PORT = 3306;
    // Networking layer requires a listening port
    private static final int TIME_BETWEEN_REQUESTS_MSEC = 10 * 1000; // 10 sec

    private AkibanConnection connection;
    private String akserverHost;
    private int akserverPort;
    private boolean monitor = false;
    private boolean resume = false;
    private boolean cleanup = false;
    private String artifactsSchema;
    private String dbHost;
    private int dbPort;
    private String dbUser;
    private String dbPassword;
    private final Map<String, String> sourceSchemas = new HashMap<String, String>();
    private final List<String> groups = new ArrayList<String>();
    private int lastEventId = -1;
}
