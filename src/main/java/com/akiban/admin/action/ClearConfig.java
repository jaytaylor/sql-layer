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
import java.util.Collection;

import com.akiban.admin.Admin;
import com.akiban.admin.AdminKey;
import com.akiban.admin.config.AkServerNetworkConfig;

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
            Collection<AkServerNetworkConfig> akServers = admin.clusterConfig().chunkservers().values();
            for (String key : AdminKey.CONFIG_KEYS) {
                admin.delete(key, -1);
            }
            // Delete /config itself
            admin.deleteDirectory(AdminKey.CONFIG_BASE);
            // Delete /state files
            for (AkServerNetworkConfig akServer : akServers) {
                admin.delete(AdminKey.stateChunkserverName(akServer.name()), -1);
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