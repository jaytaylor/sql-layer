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
