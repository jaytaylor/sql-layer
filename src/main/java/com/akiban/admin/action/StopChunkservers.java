package com.akiban.admin.action;

import com.akiban.admin.Admin;
import com.akiban.admin.config.ChunkserverNetworkConfig;
import com.akiban.admin.config.ClusterConfig;
import com.akiban.util.Command;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public abstract class StopChunkservers
{
    public static synchronized StopChunkservers only() throws IOException
    {
        if (only == null) {
            only = Admin.only().real() ? new Real() : new Fake();
        }
        return only;
    }

    public abstract void run() throws Exception;

    public final void shutdown()
    {}

    private static final Log logger = LogFactory.getLog(StopChunkservers.class);
    private static StopChunkservers only;

    private static class Real extends StopChunkservers
    {
        public void run() throws Command.Exception, IOException
        {
            ClusterConfig clusterConfig = Admin.only().clusterConfig();
            for (ChunkserverNetworkConfig chunkserver : clusterConfig.chunkservers().values()) {
                logger.info(String.format("Stopping %s", chunkserver));
                Command command = Command.printOutput(System.out,
                                                      "ssh",
                                                      "root@localhost",
                                                      "service",
                                                      "chunkserverd",
                                                      "stop",
                                                      Integer.toString(chunkserver.address().port()));
                int status = command.run();
                logger.info(String.format("Stopping %s, status: %s", chunkserver, status));
            }
        }
    }

    private static class Fake extends StopChunkservers
    {
        public void run() throws Command.Exception, IOException
        {
            logger.info("Killing all chunkservers");
            Command command = Command.printOutput(System.out,
                                                  "pkill",
                                                  "-9",
                                                  "-f",
                                                  Admin.only().chunkserverConfig().jarFile());
            int status = command.run();
            logger.info(String.format("Killed all chunkservers, status: %s", status));
        }
    }
}