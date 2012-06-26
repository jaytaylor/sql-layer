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

import com.akiban.admin.state.AkServerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.admin.config.AkServerConfig;
import com.akiban.admin.config.ClusterConfig;

public abstract class Admin
{
    // Admin interface

    public static synchronized Admin only()
    {
        // The akiban.admin system property should be one of the following:
        // - HOSTNAME: Host name or IP address of the zookeeper service. Zookeeper must be running on its default
        //       port, 2181.
        // - HOSTNAME:PORT: Host name or IP address, and port, of the zookeeper service
        // - CONFIG_PATH: For simple tests, admin is file based. Files are located under CONFIG_PATH.
        if (only == null) {
            String adminInitializer = System.getProperty(AKIBAN_ADMIN);
            if (adminInitializer == null) {
                throw new Admin.RuntimeException(String.format("System property %s must be specified.", AKIBAN_ADMIN));
            } else if (fileBased(adminInitializer)) {
                only = new FileBasedAdmin(adminInitializer);
            } else {
                only = new ZookeeperBasedAdmin(adminInitializer);
            }
        }
        return only;
    }

    public void close() throws InterruptedException, IOException
    {}

    private static boolean fileBased(String akibanAdmin)
    {
        return
            akibanAdmin.startsWith("/") || // Unix absolute
            akibanAdmin.startsWith(".") || // relative
            akibanAdmin.matches("[A-Za-z]:\\\\.*"); // Windows absolute
    }

    public abstract boolean real();

    /**
     * Sets a new value for a given key. The version number indicates the version that the caller believes is current.
     * If it is, then the value is updated and the return value is true. Otherwise, the value is not updated and the
     * return value is false. If version is null, then the caller specifies that the key does not already exist and
     * should be created. The return value is true if there was no existing version and the initial version was
     * created, false otherwise.
     *
     * @param adminKey The key whose value is being set.
     * @param version  Identifies the version being updated.
     * @param value    The value to be associated with the key
     * @return true if the value was assigned, false otherwise.
     *         if the version number supplied is not the current version number.
     * @throws com.akiban.admin.Admin.StaleUpdateException
     *
     */
    public abstract boolean set(final String adminKey, final Integer version, final String value)
        throws StaleUpdateException;

    public abstract boolean delete(final String adminKey, final Integer version)
        throws StaleUpdateException;

    public abstract boolean deleteDirectory(final String adminKey)
        throws StaleUpdateException;

    public abstract AdminValue get(final String adminKey);

    public abstract void register(final String adminKey, final Handler handler);

    public abstract void unregister(final String adminKey, final Handler handler);

    public abstract void markChunkserverUp(String chunkserverName) throws StaleUpdateException;

    public abstract void markChunkserverDown(String chunkserverName) throws StaleUpdateException;

    public final String initializer()
    {
        return adminInitializer;
    }

    public final ClusterConfig clusterConfig()
    {
        return new ClusterConfig(get(AdminKey.CONFIG_CLUSTER));
    }

    public final AkServerConfig chunkserverConfig()
    {
        return new AkServerConfig(get(AdminKey.CONFIG_CHUNKSERVER));
    }

    // For use by this package

    final AkServerState chunkserverState(String chunkserverName) throws UnknownHostException
    {
        String chunkserverStateName = AdminKey.stateChunkserverName(chunkserverName);
        return new AkServerState(get(chunkserverStateName));
    }

    final void saveChunkserverState(String chunkserverName, AkServerState akServerState)
        throws StaleUpdateException
    {
        String chunkserverStateName = AdminKey.stateChunkserverName(chunkserverName);
        set(chunkserverStateName, akServerState.version(), akServerState.toPropertiesString());
    }

    // For use by subclasses

    protected abstract boolean ensurePathExists(String leafPath, byte[] leafValue) throws InterruptedException;

    protected Admin(String adminInitializer)
    {
        this.adminInitializer = adminInitializer;
    }

    protected void checkKey(String adminKey)
    {
        for (String key : AdminKey.REQUIRED_KEYS) {
            if (key.equals(adminKey)) {
                return;
            }
        }
        if (adminKey.startsWith(AdminKey.STATE_BASE)) {
            return;
        }
        throw new BadKeyException(adminKey);
    }

    protected void checkDirectoryKey(String adminKey)
    {
        if (!(adminKey.equals(AdminKey.CONFIG_BASE) || adminKey.equals(AdminKey.STATE_BASE))) {
            throw new BadKeyException(adminKey);
        }
    }

    // For testing.
    protected static synchronized void forget() throws InterruptedException, IOException {
        if (only != null) {
            only.close();
            only = null;
        }
    }

    // State

    public static final String AKIBAN_ADMIN = "akiban.admin";

    protected static final byte[] EMPTY_VALUE = new byte[0];
    protected static final Logger logger = LoggerFactory.getLogger(Admin.class);
    private static Admin only = null;

    private final String adminInitializer;
    private boolean closed = false;

    // Inner classes

    public interface Handler
    {
        void handle(AdminValue adminValue);
    }

    abstract class Action<T>
    {
        protected abstract T action() throws Exception;

        public Action(String method, Object label)
        {
            this.method = method;
            this.label = label;
        }

        public T run()
        {
            T output = null;
            try {
                logger.info(String.format("Running %s for %s", method, label));
                output = action();
                logger.info(String.format("Completed running %s for %s: %s", method, label, output));
            } catch (InterruptedException e) {
                logger.warn(String.format("Caught %s during %s for %s", e.getClass().getName(), method, label), e);
            } catch (Exception e) {
                String message = String.format("Caught %s during %s for %s", e.getClass().getName(), method, label);
                logger.warn(message, e);
                throw new Admin.RuntimeException(message, e);
            }
            return output;
        }

        private final String method;
        private final Object label;
    }

    public static class RuntimeException extends java.lang.RuntimeException
    {
        public RuntimeException(String message)
        {
            super(message);
        }

        public RuntimeException(String message, Exception exception)
        {
            super(message, exception);
        }
    }

    public static class StaleUpdateException extends java.lang.Exception
    {
        public StaleUpdateException(String adminKey, Integer version, Exception e)
        {
            super(String.format("Cannot set %s, version %s, because there is a newer version.", adminKey, version), e);
        }
    }

    public static class BadKeyException extends RuntimeException
    {
        public BadKeyException(String key)
        {
            super(key);
        }
    }
}
