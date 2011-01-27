package com.akiban.cserver.manage;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import com.akiban.cserver.CServer;
import com.akiban.cserver.CustomQuery;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.store.Store;
import com.akiban.util.Strings;
import org.apache.log4j.Logger;

public class ManageMXBeanImpl implements ManageMXBean
{
    private static final Logger LOG = Logger.getLogger(ManageMXBeanImpl.class);
    private static final String VERSION_STRING_FILE = "version/akserver_version";
    private final String versionString;
    private final CServer cserver;
    
    private Class customClass;

    public ManageMXBeanImpl(final CServer cserver) {
        this.cserver = cserver;
        String version;
        try {
            version = Strings.join(Strings.dumpResource(null, VERSION_STRING_FILE));
        } catch (IOException e) {
            LOG.warn("Couldn't read resource file");
            version = "Error: " + e;
        }
        versionString = version;
    }

    @Override
    public void ping() {
        return;
    }

    @Override
    public void shutdown() {
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }
        }.start();
    }

    @Override
    public int getNetworkPort() {
        return cserver.port();
    }

    @Override
    public int getJmxPort() {
        int jmxPort = Integer
                .getInteger("com.sun.management.jmxremote.port", 0);
        return jmxPort;
    }

    @Override
    public boolean isVerboseLoggingEnabled() {
        return cserver.getServiceManager().getStore().isVerbose();
    }

    @Override
    public void disableVerboseLogging() {
        cserver.getServiceManager().getStore().setVerbose(false);
    }

    @Override
    public void enableVerboseLogging() {
        cserver.getServiceManager().getStore().setVerbose(true);
    }

    @Override
    public boolean isDeferIndexesEnabled() {
        return cserver.getServiceManager().getStore().isDeferIndexes();
    }

    @Override
    public void setDeferIndexes(final boolean defer) {
        getStore().setDeferIndexes(defer);
    }

    @Override
    public void buildIndexes(final String arg) {
        getStore().buildIndexes(new SessionImpl(), arg);
    }

    @Override
    public void deleteIndexes(final String arg) {
        getStore().deleteIndexes(new SessionImpl(), arg);
    }

    @Override
    public void flushIndexes() {
        getStore().flushIndexes(new SessionImpl());
    }

   
    public String loadCustomQuery(final String className, String path) {
        try {
            customClass = null;
            URL[] urls;
            if (path == null) {
                urls = new URL[]{new URL("file:///tmp/custom-classes/")};
            } else {
                String[] pathElements = path.split(":");
                urls = new URL[pathElements.length];
                for (int i = 0; i < pathElements.length; i++) {
                    urls[i] = new URL("file://" + pathElements[i]);
                }
            }
            final ClassLoader cl = new URLClassLoader(urls);
            final Class<?> c = cl.loadClass(className);
            if (CustomQuery.class.isAssignableFrom(c)) {
                customClass = c;
                return "OK";
            } else {
                return c.getSimpleName() + " does not implement CustomQuery";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return e.toString();
        }
    }
    
    public String runCustomQuery(final String params) {
        try {
            final CustomQuery cq = (CustomQuery)(customClass.newInstance());
            cq.setServiceManager(cserver.getServiceManager());
            cq.setParameters(params.split(" "));
            cq.runQuery();
            return cq.getResult();
        } catch (Exception e) {
            e.printStackTrace();
            return e.toString();
        }
    }

    private Store getStore()
    {
        return cserver.getServiceManager().getStore();
    }

    @Override
    public String getVersionString() {
        return versionString;
    }
}
