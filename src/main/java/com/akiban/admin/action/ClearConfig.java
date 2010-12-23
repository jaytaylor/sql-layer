package com.akiban.admin.action;

import java.io.IOException;
import java.util.Collection;

import com.akiban.admin.Admin;
import com.akiban.admin.AdminKey;
import com.akiban.admin.config.ChunkserverNetworkConfig;

public abstract class ClearConfig
{
    public static synchronized ClearConfig only() throws IOException
    {
        if (only == null) {
            only = Admin.only().real() ? new Real() : new Fake();
        }
        return only;
    }

    public abstract void run() throws Exception;

    public void shutdown()
    {}

    private static ClearConfig only;

    private static class Real extends ClearConfig
    {
        public void run() throws IOException, Admin.StaleUpdateException
        {
            // Delete /config files
            Admin admin = Admin.only();
            Collection<ChunkserverNetworkConfig> chunkservers = admin.clusterConfig().chunkservers().values();
            for (String key : AdminKey.CONFIG_KEYS) {
                admin.delete(key, -1);
            }
            // Delete /config itself
            admin.deleteDirectory(AdminKey.CONFIG_BASE);
            // Delete /state files
            for (ChunkserverNetworkConfig chunkserver : chunkservers) {
                admin.delete(AdminKey.stateChunkserverName(chunkserver.name()), -1);
            }
            // Delete /state itself
            admin.deleteDirectory(AdminKey.STATE_BASE);
        }
    }

    private static class Fake extends ClearConfig
    {
        public void run() throws IOException, Admin.StaleUpdateException
        {
            assert false : "not implemented yet";
        }
    }
}