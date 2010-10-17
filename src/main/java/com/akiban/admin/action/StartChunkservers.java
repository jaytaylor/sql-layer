package com.akiban.admin.action;

import com.akiban.admin.Admin;
import com.akiban.admin.config.ChunkserverConfig;
import com.akiban.admin.config.ChunkserverNetworkConfig;
import com.akiban.admin.config.ClusterConfig;
import com.akiban.util.Command;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    private static final Log logger = LogFactory.getLog(StartChunkservers.class);
    private static StartChunkservers only;

    private static class Real extends StartChunkservers
    {
        public void run() throws Command.Exception, IOException
        {
            ClusterConfig clusterConfig = Admin.only().clusterConfig();
            for (ChunkserverNetworkConfig chunkserver : clusterConfig.chunkservers().values()) {
                logger.info(String.format("Starting %s", chunkserver));
                Command command =
                    Command.printOutput(System.out,
                                        "ssh",
                                        "root@localhost",
                                        "service",
                                        "chunkserverd",
                                        "start",
                                        Integer.toString(chunkserver.address().port()),
                                        chunkserver.name(),
                                        clusterConfig.zookeeper().host().getHostAddress());
                int status = command.run();
                logger.info(String.format("Starting %s, status: %s", chunkserver, status));
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
                        ChunkserverConfig chunkserverConfig = Admin.only().chunkserverConfig();
                        String akibanAdmin = String.format("-Dakiban.admin=%s", Admin.only().initializer());
                        String maxHeap = String.format("-Xmx%sm", chunkserverConfig.maxHeapMB());
                        String jarFileLocation = String.format("%s/chunk-server/%s",
                                                               chunkserverConfig.mysqlInstallDir(),
                                                               chunkserverConfig.jarFile());
                        for (ChunkserverNetworkConfig chunkserverNetworkConfig : clusterConfig.chunkservers().values()) {
                            logger.info(String.format("Starting %s", chunkserverNetworkConfig));
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
                            logger.info(String.format("%s exited, status: %s", chunkserverNetworkConfig, status));
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
