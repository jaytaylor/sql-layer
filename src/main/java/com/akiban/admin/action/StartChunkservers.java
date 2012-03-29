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
