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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

class AdminWatcher extends Thread implements Watcher
{
    // Thread interface

    @Override
    public void run()
    {
        while (true) {
            synchronized (this) {
                while (event == null) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        logger.warn(String.format("Caught %s while waiting for event", e.getClass()), e);
                    }
                }
                logger.info(String.format("AdminWatcher thread waking up to handle %s", event));
                admin.new Action("AdminWatcher.run", event)
                {
                    @Override
                    protected Object action() throws KeeperException, InterruptedException
                    {
                        final String adminKey = event.getPath();
                        final List<Admin.Handler> handlers = admin.handlers.get(adminKey);
                        if (handlers != null) {
                            Stat stat = admin.zookeeper.exists(adminKey, true);
                            if (stat == null) {
                                logger.warn(String.format("%s does not exist.", adminKey));
                            } else {
                                byte[] bytes = admin.zookeeper.getData(adminKey, true, stat);
                                if (bytes == null) {
                                    logger.error(String.format("Null value for %s", adminKey));
                                } else {
                                    final AdminValue adminValue = new AdminValue(adminKey, stat.getVersion(), bytes);
                                    for (final Admin.Handler handler : handlers) {
                                        threadPool.execute(new Runnable()
                                        {
                                            public void run()
                                            {
                                                handler.handle(adminValue);
                                            }
                                        });
                                    }
                                }
                            }
                        }
                        return null;
                    }
                }.run();
                event = null;
            }
        }
    }


    // Watcher interface

    public void process(WatchedEvent event)
    {
        logger.info(String.format("Admin event: %s", event));
        final String adminKey = event.getPath();
        if (admin.handlers.containsKey(adminKey)) {
            synchronized (this) {
                this.event = event;
                notify();
            }
        }
    }

    // AdminWatcher interface

    AdminWatcher(ZookeeperBasedAdmin admin)
    {
        this.admin = admin;
    }

    // State

    private static final Logger logger = LoggerFactory.getLogger(AdminWatcher.class);

    private final ZookeeperBasedAdmin admin;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private WatchedEvent event;
}
