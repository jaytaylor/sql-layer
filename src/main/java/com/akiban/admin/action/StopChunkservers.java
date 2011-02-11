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

import com.akiban.admin.config.AkServerNetworkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.admin.Admin;
import com.akiban.admin.config.ClusterConfig;
import com.akiban.util.Command;

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

    private static final Logger logger = LoggerFactory.getLogger(StopChunkservers.class);
    private static StopChunkservers only;

    private static class Real extends StopChunkservers
    {
        public void run() throws Command.Exception, IOException
        {
            ClusterConfig clusterConfig = Admin.only().clusterConfig();
            for (AkServerNetworkConfig akServer : clusterConfig.chunkservers().values()) {
                logger.info(String.format("Stopping %s", akServer));
                Command command = Command.printOutput(System.out,
                                                      "ssh",
                                                      "root@localhost",
                                                      "service",
                                                      "chunkserverd",
                                                      "stop",
                                                      Integer.toString(akServer.address().port()));
                int status = command.run();
                logger.info(String.format("Stopping %s, status: %s", akServer, status));
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
