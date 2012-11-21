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

package com.akiban.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;

public class GCMonitor extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(GCMonitor.class);
    private static final String THREAD_NAME = "GC_MONITOR";
    private static final String LOG_MSG_FORMAT = "%s spent %s ms on %s collections, heap at %.2f%%";

    private static class GCInfo {
        public final GarbageCollectorMXBean mxBean;
        public long lastCollectionCount = 0;
        public long lastCollectionTime = 0;

        public GCInfo(GarbageCollectorMXBean mxBean) {
            this.mxBean = mxBean;
        }

        public long updateCollectionTime() {
            long prev = lastCollectionTime;
            lastCollectionTime = mxBean.getCollectionTime();
            return lastCollectionTime - prev;
        }

        public long updateCollectionCount() {
            long prev = lastCollectionCount;
            lastCollectionCount = mxBean.getCollectionCount();
            return lastCollectionCount - prev;
        }
    }

    private final int interval;
    private final int logThreshold;
    private final List<GCInfo> gcInfo = new ArrayList<GCInfo>();
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private volatile boolean running;


    public GCMonitor(int interval, int logThreshold) {
        super(THREAD_NAME);
        this.interval = interval;
        this.logThreshold = logThreshold;
        for(GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcInfo.add(new GCInfo(gcBean));
        }
    }

    @Override
    public void run() {
        running = !gcInfo.isEmpty();
        while(running) {
            runInternal();
            try {
                Thread.sleep(interval);
            } catch(InterruptedException e) {
                LOG.debug("Interrupted, exiting", e);
                running = false;
            }
        }
    }

    public void stopRunning() {
        this.running = false;
        interrupt();
    }

    private void runInternal() {
        for(GCInfo info : gcInfo) {
            long elapsed = info.updateCollectionTime();
            long collections = info.updateCollectionCount();

            if(collections == 0) {
                continue;
            }

            long timePerCollection = elapsed / collections;
            boolean doWarn = (timePerCollection >= logThreshold);
            boolean doDebug = LOG.isDebugEnabled();

            if(doWarn || doDebug) {
                MemoryUsage usage = memoryBean.getHeapMemoryUsage();
                double percent = 100 * ((double)usage.getUsed() / (double)usage.getMax());
                String message =  String.format(LOG_MSG_FORMAT, info.mxBean.getName(), elapsed, collections, percent);
                if(doWarn) {
                    LOG.warn(message);
                } else {
                    LOG.debug(message);
                }
            }

            // Anything we can do to try and reduce pressure?
        }
    }
}
