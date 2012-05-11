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

package com.akiban.admin;

import java.io.IOException;
import java.util.Map;

import com.akiban.admin.state.AkServerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.mortbay.jetty.Server;

import com.akiban.admin.action.ClearConfig;
import com.akiban.admin.action.StartChunkservers;
import com.akiban.admin.action.StopChunkservers;
import com.akiban.admin.config.AkServerNetworkConfig;
import com.akiban.admin.config.ClusterConfig;

public class AdminService
{
    public static void main(String[] args) throws Exception
    {
        new AdminService().run();
    }

    private AdminService() throws Exception
    {
        admin = Admin.only();
    }

    private void run() throws Exception
    {
        ensureRequiredKeysExist();
        watchClusterConfig();
        contactMySQLHead();
        startHTTPInterface();
    }

    private void stop() throws InterruptedException, IOException
    {
        StartChunkservers.only().shutdown();
        StopChunkservers.only().shutdown();
        ClearConfig.only().shutdown();
    }

    private void ensureRequiredKeysExist() throws Admin.StaleUpdateException
    {
        logger.info("initializing config keys");
        for (String adminKey : AdminKey.REQUIRED_KEYS) {
            AdminValue adminValue = admin.get(adminKey);
            if (adminValue == null) {
                admin.set(adminKey, null, "");
            }
        }
    }

    private void watchClusterConfig()
    {
        logger.info("setting watch for cluster config");
        admin.register(AdminKey.CONFIG_CLUSTER,
                       new Admin.Handler()
                       {
                           @Override
                           public void handle(AdminValue adminValue)
                           {
                               handleClusterConfigUpdate();
                           }
                       });
    }

    private void contactMySQLHead()
    {
        Thread thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                while (true) {
                    Exception exception = null;
                    logger.debug("Sending admin info to mysql head");
                    try {
                        sayHelloToMySQL();
                    } catch (Exception e) {
                        exception = e;
                    }
                    if (exception != null) {
                        logger.warn("Caught exception while sending admin info to mysql head", exception);
                    }
                    try {
                        Thread.sleep(MYSQL_RETRY_DELAY_MSEC);
                    } catch (InterruptedException e) {
                        logger.warn("Caught InterruptedException while sleeping before contacting mysql", e);
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private void sayHelloToMySQL() throws Exception
    {
        throw new UnsupportedOperationException();

        /* If this used again, rewrite in terms of AkibanConnectionImpl

        ClusterConfig cluster = Admin.only().clusterConfig();
        Address mysql = cluster.mysql();
        if (mysql != null) { // mysql not known until cluster config has actually been loaded
            AkibanConnection mysqlConnection =
                NettyAkibanConnectionImpl.createConnection(NetworkHandlerFactory.getHandler(mysql.host().getHostAddress(),
                                                                                      Integer.toString(mysql.port()),
                                                                                      null));
            AdminIntroductionRequest request = new AdminIntroductionRequest(admin.initializer());
            logger.info("Sending AdminIntroductionRequest to mysql head");
            try {
                AdminIntroductionResponse response = (AdminIntroductionResponse) mysqlConnection.sendAndReceive(request);
                logger.info("AdminIntroductionResponse received from mysql head");
            } finally {
                mysqlConnection.close();
            }
        } // else: mysql not known before initial config has been loaded. Caller will try again.

        */
    }

    private void handleClusterConfigUpdate()
    {
        logger.warn("Responding to updated cluster config");
        Exception exception = null;
        try {
            ClusterConfig clusterConfig = admin.clusterConfig();
            // clusterConfig.chunkservers() describes the chunkservers that are expected to be present.
            for (Map.Entry<String, AkServerNetworkConfig> entry : clusterConfig.chunkservers().entrySet()) {
                String chunkserverName = entry.getKey();
                AkServerNetworkConfig akServerNetworkConfig = entry.getValue();
                // Get the chunkserver state file, if it exists
                String chunkserverStateName = AdminKey.stateChunkserverName(chunkserverName);
                AdminValue value = admin.get(chunkserverStateName);
                if (value == null) {
                    // Initial state of unknown chunkserver *should* be down.
                    admin.set(chunkserverStateName,
                              null,
                              new AkServerState(false, akServerNetworkConfig.lead()).toPropertiesString());
                }
            }
        } catch (Admin.StaleUpdateException e) {
            exception = e;
        }
        if (exception != null) {
            logger.warn
                (String.format("Caught %s while handling cluster config update", exception.getClass().getName()),
                 exception);
        }
    }

    private void startHTTPInterface()
        throws Exception
    {
        server = new Server(DEFAULT_ADMIN_HTTP_PORT);
        server.setHandler(new AdminHTTPHandler());
        server.start();
    }

    public static final int DEFAULT_ADMIN_SERVICE_PORT = 8764;
    public static final int DEFAULT_ADMIN_HTTP_PORT = 8765;

    private static final Logger logger = LoggerFactory.getLogger(AdminService.class);
    private static final String LOCALHOST = "localhost";
    private static final int MYSQL_RETRY_DELAY_MSEC = 5000;

    private Admin admin;
    private Server server;
}
