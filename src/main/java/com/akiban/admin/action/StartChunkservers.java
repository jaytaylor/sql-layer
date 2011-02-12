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

package com.akiban.admin.action;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.akiban.admin.config.AkServerConfig;
import com.akiban.admin.config.AkServerNetworkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.admin.Admin;
import com.akiban.admin.config.ClusterConfig;
import com.akiban.util.Command;

public abstract class StartChunkservers
{
    public static synchronized StartChunkservers only() throws IOException
    {
        if (only == null) {
            only = Admin.only().real() ? new Real() : new Fake();
        }
        return only;
    }

    public abstract void run() throws Exception;

    public abstract void shutdown();

    private static final Logger logger = LoggerFactory.getLogger(StartChunkservers.class);
    private static StartChunkservers only;

    private static class Real extends StartChunkservers
    {
        public void run() throws Command.Exception, IOException
        {
            ClusterConfig clusterConfig = Admin.only().clusterConfig();
            for (AkServerNetworkConfig akServer : clusterConfig.chunkservers().values()) {
                logger.info(String.format("Starting %s", akServer));
                Command command =
                    Command.printOutput(System.out,
                                        "ssh",
                                        "root@localhost",
                                        "service",
                                        "chunkserverd",
                                        "start",
                                        Integer.toString(akServer.address().port()),
                                        akServer.name(),
                                        clusterConfig.zookeeper().host().getHostAddress());
                int status = command.run();
                logger.info(String.format("Starting %s, status: %s", akServer, status));
            }
        }

        public void shutdown()
        {}
    }

    private static class Fake extends StartChunkservers
    {
        public void run() throws Command.Exception, IOException
        {
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        ClusterConfig clusterConfig = Admin.only().clusterConfig();
                        AkServerConfig akServerConfig = Admin.only().chunkserverConfig();
                        String akibanAdmin = String.format("-Dakiban.admin=%s", Admin.only().initializer());
                        String maxHeap = String.format("-Xmx%sm", akServerConfig.maxHeapMB());
                        String jarFileLocation = String.format("%s/chunk-server/%s",
                                                               akServerConfig.mysqlInstallDir(),
                                                               akServerConfig.jarFile());
                        for (AkServerNetworkConfig akServerNetworkConfig : clusterConfig.chunkservers().values()) {
                            logger.info(String.format("Starting %s", akServerNetworkConfig));
                            // Options copied from system-test/build.xml, start-chunkserver-target, except:
                            // - com.persistit.showgui omitted
                            // - com.akiban.config omitted, so that config is loaded from admin
                            Command command =
                                Command.printOutput(System.out,
                                                    "java",
                                                    "-Xrunjdwp:transport=dt_socket,address=8000,suspend=n,server=y",
                                                    "-Xdebug",
                                                    "-Xnoagent",
                                                    "-Djava.compiler=NONE",
                                                    akibanAdmin,
                                                    maxHeap,
                                                    "-jar",
                                                    jarFileLocation);
                            int status = command.run();
                            logger.info(String.format("%s exited, status: %s", akServerNetworkConfig, status));
                        }
                    } catch (Exception e) {
                        logger.error("Caught exception while starting chunkservers", e);
                    }
                }
            });
        }

        public void shutdown()
        {
            executor.shutdown();
        }

        private ExecutorService executor = Executors.newSingleThreadExecutor();
    }
}
