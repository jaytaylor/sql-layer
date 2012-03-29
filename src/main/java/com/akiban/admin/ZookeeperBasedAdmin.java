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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.akiban.admin.state.AkServerState;
import com.akiban.server.error.ZooKeeperInitFailureException;

public class ZookeeperBasedAdmin extends Admin
{
    // Admin interface

    @Override
    public boolean real()
    {
        return true;
    }

    public void close() throws InterruptedException, IOException
    {
        super.close();
        zookeeper.close();
    }

    public boolean set(final String adminKey, final Integer version, final String value)
        throws StaleUpdateException
    {
        return new Action<Boolean>("set", adminKey)
        {
            @Override
            protected Boolean action() throws Exception
            {
                boolean updated;
                checkKey(adminKey);
                byte[] bytes = value.getBytes();
                if (version == null) {
                    updated = ensurePathExists(adminKey, bytes); // returns true iff creates leaf
                } else {
                    try {
                        zookeeper.setData(adminKey, bytes, version);
                        updated = true;
                    } catch (KeeperException.NoNodeException e) {
                        // This is strange. Caller presumably saw a value, since a version number was provided.
                        // But now the key doesn't exist.
                        updated = false;
                    } catch (KeeperException.BadVersionException e) {
                        throw new StaleUpdateException(adminKey, version, e);
                    }
                }
                return updated;
            }
        }.run();
    }

    public boolean delete(final String adminKey, final Integer version)
        throws StaleUpdateException
    {
        return new Action<Boolean>("delete", adminKey)
        {
            @Override
            protected Boolean action() throws Exception
            {
                boolean updated;
                checkKey(adminKey);
                if (version == null) {
                    // Strange request -- deleting something believed not to exist. Nothing to do.
                    updated = false;
                } else {
                    try {
                        zookeeper.delete(adminKey, version);
                        updated = true;
                    } catch (KeeperException.NoNodeException e) {
                        // This is strange. Caller presumably saw a value, since a version number was provided.
                        // But now the key doesn't exist.
                        updated = false;
                    } catch (KeeperException.BadVersionException e) {
                        throw new StaleUpdateException(adminKey, version, e);
                    }
                }
                return updated;
            }
        }.run();
    }

    public boolean deleteDirectory(final String adminKey)
        throws StaleUpdateException
    {
        return new Action<Boolean>("deleteDirectory", adminKey)
        {
            @Override
            protected Boolean action() throws Exception
            {
                boolean updated;
                checkDirectoryKey(adminKey);
                try {
                    zookeeper.delete(adminKey, -1);
                    updated = true;
                } catch (KeeperException.NoNodeException e) {
                    // This is strange. Caller presumably saw a value, since a version number was provided.
                    // But now the key doesn't exist.
                    updated = false;
                } catch (KeeperException.BadVersionException e) {
                    throw new StaleUpdateException(adminKey, -1, e);
                }
                return updated;
            }
        }.run();
    }

    public AdminValue get(final String adminKey)
    {
        checkKey(adminKey);
        return new Action<AdminValue>("get", adminKey)
        {
            @Override
            protected AdminValue action() throws Exception
            {
                AdminValue value;
                Stat stat = new Stat();
                try {
                    byte[] bytes = zookeeper.getData(adminKey, true, stat);
                    value = new AdminValue(adminKey, stat.getVersion(), bytes);
                } catch (KeeperException.NoNodeException e) {
                    value = null;
                }
                return value;
            }
        }.run();
    }

    public void register(final String adminKey, final Handler handler)
    {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("Registering %s for %s", handler, adminKey));
        }
        checkKey(adminKey);
        new Action("register", adminKey)
        {
            @Override
            protected Object action() throws Exception
            {
                Stat stat = zookeeper.exists(adminKey, true);
                if (stat == null) {
                    logger.error(String.format("Registration for %s failed, key does not exist", adminKey));
                } else {
                    List<Handler> keyHandlers = handlers.get(adminKey);
                    if (keyHandlers == null) {
                        keyHandlers = new ArrayList<Handler>();
                        handlers.put(adminKey, keyHandlers);
                    }
                    keyHandlers.add(handler);
                    byte[] data = zookeeper.getData(adminKey, true, stat);
                    handler.handle(new AdminValue(adminKey, stat.getVersion(), data));
                }
                return null;
            }
        }.run();
    }

    public void unregister(final String adminKey, final Handler handler)
    {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("Unregistering %s for %s", handler, adminKey));
        }
        checkKey(adminKey);
        new Action("unregister", adminKey)
        {
            @Override
            protected Object action() throws InterruptedException
            {
                List<Handler> keyHandlers = handlers.get(adminKey);
                if (keyHandlers == null) {
                    logger.warn(String.format("No registration for %s", adminKey));
                } else {
                    boolean removed = keyHandlers.remove(handler);
                    if (!removed) {
                        logger.warn(String.format("%s not registered for %s", handler, adminKey));
                    }
                }
                return null;
            }
        }.run();
    }

    public void markChunkserverUp(String chunkserverName) throws StaleUpdateException
    {
        logger.info(String.format("Setting chunkserver state for %s to up",
                                  AdminKey.stateChunkserverName(chunkserverName)));
        try {
            AkServerState akServerState = chunkserverState(chunkserverName);
            akServerState.up(true);
            saveChunkserverState(chunkserverName, akServerState);
        } catch (UnknownHostException e) {
            throw new RuntimeException
                (String.format("Caught exception while marking %s as up", chunkserverName), e);
        }
    }

    public void markChunkserverDown(String chunkserverName) throws StaleUpdateException
    {
        logger.info(String.format("Setting chunkserver state for %s to down",
                                  AdminKey.stateChunkserverName(chunkserverName)));
        try {
            AkServerState akServerState = chunkserverState(chunkserverName);
            akServerState.up(false);
            saveChunkserverState(chunkserverName, akServerState);
        } catch (UnknownHostException e) {
            throw new RuntimeException
                (String.format("Caught exception while marking %s as down", chunkserverName), e);
        }
    }

    protected boolean ensurePathExists(String leafPath, byte[] leafValue) throws InterruptedException
    {
        boolean createdLeaf = false;
        StringBuilder buffer = new StringBuilder();
        StringTokenizer scanner = new StringTokenizer(leafPath, "/");
        while (scanner.hasMoreTokens()) {
            buffer.append("/");
            buffer.append(scanner.nextToken());
            String path = buffer.toString();
            boolean atLeaf = path.equals(leafPath);
            try {
                zookeeper.create(path, atLeaf ? leafValue : EMPTY_VALUE, ACL, CreateMode.PERSISTENT);
                createdLeaf = atLeaf;
            } catch (KeeperException.NodeExistsException e) {
                createdLeaf = false;
            } catch (KeeperException e) {
                throw new Admin.RuntimeException
                    (String.format("Caught exception while ensuring existence of path %s", leafPath), e);
            }
        }
        return createdLeaf;
    }

    // For use by this package

    ZookeeperBasedAdmin(String zookeeperLocation) {
        super(zookeeperLocation);
        if (zookeeperLocation.indexOf(':') < 0) {
            zookeeperLocation += ":" + ZOOKEEPER_DEFAULT_PORT;
        }
        AdminWatcher adminWatcher = new AdminWatcher(this);
        adminWatcher.setDaemon(true);
        adminWatcher.start();
        try {
            zookeeper = new ZooKeeper(zookeeperLocation, ZOOKEEPER_SESSION_TIMEOUT_MSEC, adminWatcher);
        } catch (IOException e) {
            throw new ZooKeeperInitFailureException (zookeeperLocation, e.getMessage());
        }
        
        logger.info(String.format("Started zookeeper-based admin using zookeeper at %s", zookeeperLocation));
    }

    // For use by this class

    private static String[] hostAndPort(String s)
    {
        String[] hostAndPort = new String[2];
        int colon = s.indexOf(':');
        if (colon < 0) {
            hostAndPort[0] = s;
            hostAndPort[1] = null;
        } else {
            hostAndPort[0] = s.substring(0, colon);
            hostAndPort[1] = s.substring(colon + 1);
        }
        return hostAndPort;
    }

    // State

    private static final int ZOOKEEPER_DEFAULT_PORT = 2181;
    private static final int ZOOKEEPER_SESSION_TIMEOUT_MSEC = 30000;
    private static final List<ACL> ACL = Arrays.asList(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));

    final ZooKeeper zookeeper;
    final Map<String, List<Handler>> handlers = new HashMap<String, List<Handler>>();
}