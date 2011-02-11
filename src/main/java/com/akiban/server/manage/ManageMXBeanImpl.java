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

package com.akiban.server.manage;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicReference;

import com.akiban.server.CServer;
import com.akiban.server.CustomQuery;
import com.akiban.server.service.session.SessionImpl;
import com.akiban.server.store.Store;

public class ManageMXBeanImpl implements ManageMXBean {
    private final CServer cserver;

    private Class<?> customClass;
    private AtomicReference<CustomQuery> runningQuery = new AtomicReference<CustomQuery>();

    public ManageMXBeanImpl(final CServer cserver) {
        this.cserver = cserver;
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

    @Override
    public String loadCustomQuery(final String className, String path) {
        try {
            customClass = null;
            URL[] urls;
            if (path == null) {
                urls = new URL[] { new URL("file:///tmp/custom-classes/") };
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

    @Override
    public String runCustomQuery(final String params) {
        if (runningQuery.get() == null) {
            try {
                final CustomQuery cq = (CustomQuery) (customClass.newInstance());
                cq.setServiceManager(cserver.getServiceManager());
                cq.setParameters(params.split(" "));
                runningQuery.set(cq);
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            cq.runQuery();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }, "Run " + customClass.getSimpleName()).start();
                return "Running - use showCustomQueryResult to view status";
            } catch (Exception e) {
                runningQuery.set(null);
                e.printStackTrace();
                return e.toString();
            }
        } else {
            return "Already running - use stopCustomQuery to stop";
        }
    }

    @Override
    public String stopCustomQuery() {
        CustomQuery cq = runningQuery.get();
        if (cq == null) {
            return "No running query";
        } else {
            try {
                cq.stopQuery();
                runningQuery.set(null);
                return cq.getResult();
            } catch (Exception e) {
                e.printStackTrace();
                return e.toString();
            }
        }
    }
    
    @Override
    public String showCustomQueryResult() {
        CustomQuery cq = runningQuery.get();
        if (cq == null) {
            return "No running query";
        } else {
            return cq.getResult();
        }
    }

    private Store getStore() {
        return cserver.getServiceManager().getStore();
    }

    @Override
    public String getVersionString() {
        return CServer.VERSION_STRING;
    }
}
