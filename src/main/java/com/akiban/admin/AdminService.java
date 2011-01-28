/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.admin;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Server;

import com.akiban.admin.action.ClearConfig;
import com.akiban.admin.action.StartChunkservers;
import com.akiban.admin.action.StopChunkservers;
import com.akiban.admin.config.ChunkserverNetworkConfig;
import com.akiban.admin.config.ClusterConfig;
import com.akiban.admin.message.AdminIntroductionRequest;
import com.akiban.admin.message.AdminIntroductionResponse;
import com.akiban.admin.state.ChunkserverState;
import com.akiban.message.AkibanConnection;
import com.akiban.message.MessageRegistry;
import com.akiban.message.NettyAkibanConnectionImpl;
import com.akiban.network.AkibaNetworkHandler;
import com.akiban.network.CommEventNotifier;
import com.akiban.network.NetworkHandlerFactory;

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
        initializeNetwork();
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

    private void initializeNetwork()
    {
        logger.info("initializing network");
        MessageRegistry.initialize();
        MessageRegistry.only().registerModule("com.akiban.admin.message");
        // MessageRegistry.only().registerModule("com.akiban.ais.message");
        MessageRegistry.only().registerModule("com.akiban.message");
        NetworkHandlerFactory.initializeNetwork(LOCALHOST,
                                                Integer.toString(DEFAULT_ADMIN_SERVICE_PORT),
                                                new ChannelNotifier());
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
    }

    private void handleClusterConfigUpdate()
    {
        logger.warn("Responding to updated cluster config");
        Exception exception = null;
        try {
            ClusterConfig clusterConfig = admin.clusterConfig();
            // clusterConfig.chunkservers() describes the chunkservers that are expected to be present.
            for (Map.Entry<String, ChunkserverNetworkConfig> entry : clusterConfig.chunkservers().entrySet()) {
                String chunkserverName = entry.getKey();
                ChunkserverNetworkConfig chunkserverNetworkConfig = entry.getValue();
                // Get the chunkserver state file, if it exists
                String chunkserverStateName = AdminKey.stateChunkserverName(chunkserverName);
                AdminValue value = admin.get(chunkserverStateName);
                if (value == null) {
                    // Initial state of unknown chunkserver *should* be down.
                    admin.set(chunkserverStateName,
                              null,
                              new ChunkserverState(false, chunkserverNetworkConfig.lead()).toPropertiesString());
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

    private static final Log logger = LogFactory.getLog(AdminService.class);
    private static final String LOCALHOST = "localhost";
    private static final int MYSQL_RETRY_DELAY_MSEC = 5000;

    private Admin admin;
    private Server server;

    public class ChannelNotifier implements CommEventNotifier
    {
        @Override
        public void onConnect(AkibaNetworkHandler handler)
        {
            logger.error("AdminService.ChannelNotifier.onConnect shouldn't be called.");
        }

        @Override
        public void onDisconnect(AkibaNetworkHandler handler)
        {
            logger.error("AdminService.ChannelNotifier.onDisconnect shouldn't be called.");
        }
    }
}
