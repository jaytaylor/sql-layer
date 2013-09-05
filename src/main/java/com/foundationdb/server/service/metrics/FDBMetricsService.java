/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.metrics;

import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.store.FDBHolder;

import com.foundationdb.KeyValue;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.tuple.Tuple;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class FDBMetricsService implements MetricsService, Service
{
    public static final byte[] PREFIX_BYTES = { 
        (byte)0xFF, (byte)'/', (byte)'a', (byte)'/' 
    };
    public static final String METRIC_KEY = "TDMetric";
    public static final String METRIC_CONF_KEY = "TDMetricConf";
    public static final String METRIC_CONF_CHANGES_KEY = "TDMetricConfChanges";
    public static final String BOOLEAN_TYPE = "Bool";
    public static final String LONG_TYPE = "Int64";
    public static final String ENABLED_OPTION = "Enabled";
    public static final byte[] ENABLED_FALSE = { (byte)0 };
    public static final byte[] ENABLED_TRUE = { (byte)1 };
    public static final int NLEVELS = 25;
    public static final int LEVEL_VALUE_MAX_SIZE = 50000;
    public static final double METRIC_LEVEL_DIVISOR = Math.log(4);
    private static final Logger logger = LoggerFactory.getLogger(FDBMetricsService.class);
    private final ConfigurationService configService;
    private final FDBHolder fdbService;
    private final ConcurrentHashMap<String,BaseMetricImpl<?>> metrics = new ConcurrentHashMap<>();
    private long timebase;
    private String address;
    private Random random;
    protected Thread backgroundThread;
    protected boolean running, anyEnabled, confChanged, backgroundIdle;
    protected volatile boolean metricsChanged;
    private Map<Tuple,byte[]> conf;
    private List<KeyValue> pendingWrites = new ArrayList<>();

    @Inject
    public FDBMetricsService(ConfigurationService configService, FDBHolder fdbService) {
        this.configService = configService;
        this.fdbService = fdbService;
    }

    static class MetricLevel<T> {
        final Deque<MetricLevelValues> values = new ArrayDeque<>();
        T lastValue;
        long lastTime;
    }

    static class MetricLevelValues {
        final long start;
        byte[] bytes;
        
        MetricLevelValues(long start, byte[] bytes) {
            this.start = start;
            this.bytes = bytes;
        }
        
        void append(byte[] moreBytes) {
            byte[] newBytes = new byte[bytes.length + moreBytes.length];
            System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
            System.arraycopy(moreBytes, 0, newBytes, bytes.length, moreBytes.length);
            bytes = newBytes;
        }

        boolean isFull() {
            return (bytes.length >= LEVEL_VALUE_MAX_SIZE);
        }
    }

    abstract class BaseMetricImpl<T> implements FDBMetric<T> {
        private final String name;
        protected volatile boolean enabled, confChanged, valueChanged;
        protected long changeTime;
        protected final List<MetricLevel<T>> levels = new ArrayList<>(NLEVELS);
        
        protected BaseMetricImpl(String name) {
            this.name = name;
            for (int i = 0; i < NLEVELS; i++) {
                levels.add(new MetricLevel<T>());
            }
        }

        @Override
        public String getName() {
            return name;
        }
        
        @Override
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public void setEnabled(boolean enabled) {
            setEnabled(this, enabled, true);
        }

        @Override
        public Future<List<FDBMetric.Value<T>>> readAllValues(Transaction tr) {
            return readAllValues(tr, this);
        }

        protected abstract String getType();
        protected abstract byte[] encodeValue();
        protected abstract byte[] encodeValue(MetricLevel<T> onto);
        protected abstract List<FDBMetric.Value<T>> decodeValues(byte[] bytes);

        @Override
        public String toString() {
            return getType() + " " + name + " = " + getObject();
        }
    }

    class BooleanMetricImpl extends BaseMetricImpl<Boolean> implements BooleanMetric {
        private final AtomicBoolean bvalue = new AtomicBoolean();

        public BooleanMetricImpl(String name) {
            super(name);
        }

        @Override
        public boolean get() {
            return bvalue.get();
        }

        @Override
        public void set(boolean value) {
            if (!enabled) {
                bvalue.set(value);
            }
            else {
                synchronized (this) {
                    boolean ovalue = bvalue.get();
                    if (value != ovalue) {
                        if (bvalue.compareAndSet(ovalue, value)) {
                            metricChanged(this);
                        }
                    }
                }
            }
        }
        
        @Override
        public Boolean getObject() {
            return get();
        }

        @Override
        public void setObject(Boolean value) {
            set(value);
        }
        
        @Override
        public boolean toggle() {
            if (!enabled) {
                while (true) {
                    boolean value = bvalue.get();
                    boolean newValue = !value;
                    if (bvalue.compareAndSet(value, newValue)) {
                        return newValue;
                    }
                }
            }
            else {
                synchronized (this) {
                    boolean value = !get();
                    set(value);
                    return value;
                }
            }
        }

        @Override
        protected String getType() {
            return BOOLEAN_TYPE;
        }
    
        @Override
        protected byte[] encodeValue() {
            return Tuple.from(combine(changeTime, get())).pack();
        }

        @Override
        protected byte[] encodeValue(MetricLevel<Boolean> onto) {
            return Tuple.from(combine(changeTime, get()) -
                              combine(onto.lastTime, onto.lastValue))
                        .pack();
        }

        @Override
        protected List<FDBMetric.Value<Boolean>> decodeValues(byte[] bytes) {
            Tuple tuple = Tuple.fromBytes(bytes);
            int nvalues = tuple.size();
            List<FDBMetric.Value<Boolean>> result = new ArrayList<>(nvalues);
            for (int i = 0; i < nvalues; i++) {
                long tvalue = tuple.getLong(i);
                long time = tvalue >>> 1;
                boolean value = ((tvalue & 1) != 0);
                result.add(new FDBMetric.Value<Boolean>(time, value));
            }
            return result;
        }

        private long combine(long time, boolean value) {
            return (time * 2) + (value ? 1 : 0);
        }
    }

    class LongMetricImpl extends BaseMetricImpl<Long> implements LongMetric {
        private final AtomicLong lvalue = new AtomicLong();

        public LongMetricImpl(String name) {
            super(name);
        }

        @Override
        public long get() {
            return lvalue.get();
        }

        @Override
        public void set(long value) {
            if (!enabled) {
                lvalue.set(value);
            }
            else {
                synchronized (this) {
                    long ovalue = lvalue.get();
                    if (value != ovalue) {
                        if (lvalue.compareAndSet(ovalue, value)) {
                            metricChanged(this);
                        }
                    }
                }
            }
        }
        
        @Override
        public Long getObject() {
            return get();
        }

        @Override
        public void setObject(Long value) {
            set(value);
        }
        
        @Override
        public long increment() {
            if (!enabled) {
                return lvalue.incrementAndGet();
            }
            else {
                synchronized (this) {
                    long value = get() + 1;
                    set(value);
                    return value;
                }
            }
        }
        
        @Override
        public long increment(long amount) {
            if (!enabled) {
                return lvalue.addAndGet(amount);
            }
            else {
                synchronized (this) {
                    long value = get() + amount;
                    set(value);
                    return value;
                }
            }
        }

        @Override
        protected String getType() {
            return LONG_TYPE;
        }
    
        @Override
        protected byte[] encodeValue() {
            return Tuple.from(changeTime, get()).pack();
        }

        @Override
        protected byte[] encodeValue(MetricLevel<Long> onto) {
            return Tuple.from(changeTime - onto.lastTime, 
                              get() - onto.lastValue)
                         .pack();
        }

        @Override
        protected List<FDBMetric.Value<Long>> decodeValues(byte[] bytes) {
            Tuple tuple = Tuple.fromBytes(bytes);
            int nvalues = tuple.size() / 2;
            List<FDBMetric.Value<Long>> result = new ArrayList<>(nvalues);
            long time = 0, value = 0;
            for (int i = 0; i < nvalues; i++) {
                time += tuple.getLong(i * 2);
                value += tuple.getLong(i * 2 + 1);
                result.add(new FDBMetric.Value<Long>(time, value));
            }
            return result;
        }
    }

    /* MetricCollection */

    @Override
    public BooleanMetric addBooleanMetric(String name) {
        BooleanMetricImpl metric = new BooleanMetricImpl(name);
        addMetric(metric);
        return metric;
    }

    @Override
    public LongMetric addLongMetric(String name) {
        LongMetricImpl metric = new LongMetricImpl(name);
        addMetric(metric);
        return metric;
    }

    @Override
    public <T> void removeMetric(BaseMetric<T> metric) {
        metrics.remove(metric.getName(), metric);
    }
    
    /* Service */
    
    @Override
    public void start() {
        // NOTE: Java does not expose a nanosecond wallclock timer,
        // like CLOCK_REALTIME, only one like CLOCK_MONOTONIC. (Among
        // other things, 64 bits is only 292 years.) There is a
        // tradeoff between using synchronized clocks in a multi-node
        // environment and using clocks that NTP doesn't change so
        // that small-scale deltas are always accurate.  fdbserver
        // metrics choose the former.
        timebase = System.currentTimeMillis() * 1000000 - System.nanoTime();
        address = "127.0.0.1";
        try {
            address = InetAddress.getLocalHost().getHostAddress();
        }
        catch (IOException ex) {
        }
        address = address + ":" + configService.getProperty("fdbsql.postgres.port");
        random = new Random();
        
        backgroundThread = new Thread() {
                @Override
                public void run() {
                    backgroundThread();
                }
            };
        loadConf();
        running = true;
        backgroundThread.start();
    }

    @Override
    public void stop() {
        running = false;
        notifyBackground();
        try {
            backgroundThread.join(1000);
        }
        catch (InterruptedException ex) {
        }
    }

    @Override
    public void crash() {
        stop();
    }

    /* For testing */

    public void reset() {
        metrics.clear();
        loadConf();
    }

    public void completeBackgroundWork() {
        notifyBackground();
        while (!backgroundIdle) {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException ex) {
            }
        }
    }

    public void deleteBooleanMetric(Transaction tr, String name) {
        tr.clear(Range.startsWith(metricKey(METRIC_KEY, BOOLEAN_TYPE, name)));
        tr.clear(Range.startsWith(metricKey(METRIC_CONF_KEY, BOOLEAN_TYPE, name)));
    }

    public void deleteLongMetric(Transaction tr, String name) {
        tr.clear(Range.startsWith(metricKey(METRIC_KEY, LONG_TYPE, name)));
        tr.clear(Range.startsWith(metricKey(METRIC_CONF_KEY, LONG_TYPE, name)));
    }

    /* Internal */

    protected void addMetric(BaseMetricImpl<?> metric) {
        if (metrics.putIfAbsent(metric.getName(), metric) != null) {
            throw new IllegalArgumentException("There is already a metric named " + metric.getName());
        }
        updateEnabled(metric);  // Get initial enabled state from conf.
    }

    protected <T> void metricChanged(BaseMetricImpl<T> metric) {
        long lastTime = metric.changeTime;

        // NOTE: changeTime is only accurate for metrics that are
        // enabled, and to guarantee that is always corresponds to the
        // value, updates to enabled metrics are synchronized.
        metric.changeTime = timebase + System.nanoTime();
        metric.valueChanged = true;
        
        int level;
        if (lastTime == 0) {
            level = 0;
        }
        else {
            double r = random.nextDouble();
            if (r == 0) {
                level = NLEVELS-1;
            }
            else {
                level = Math.min(NLEVELS-1, (int)(Math.log((metric.changeTime - lastTime) / r) / METRIC_LEVEL_DIVISOR));
            }
        }
        MetricLevel<T> metricLevel = metric.levels.get(level);
        MetricLevelValues values = metricLevel.values.peekLast();
        if ((values == null) || values.isFull()) {
            logger.debug("New level {} entry for {}", level, metric);
            values = new MetricLevelValues(metric.changeTime, metric.encodeValue());
            metricLevel.values.addLast(values);
        }
        else {
            logger.debug("Adding to level {} entry for {}", level, metric);
            values.append(metric.encodeValue(metricLevel));
        }
        metricLevel.lastValue = metric.getObject();
        metricLevel.lastTime = metric.changeTime;
    }

    protected void notifyBackground() {
        if (Thread.currentThread() != backgroundThread) {
            synchronized (backgroundThread) {
                backgroundThread.notifyAll();
            }
        }
    }

    protected void backgroundThread() {
        try {
            while (running) {
                if (!confChanged && !metricsChanged) {
                    try {
                        synchronized (backgroundThread) {
                            backgroundIdle = true;
                            if (anyEnabled)
                                backgroundThread.wait(1000);
                            else
                                backgroundThread.wait();
                            backgroundIdle = false;
                        }
                    }
                    catch (InterruptedException ex) {
                        break;
                    }
                }
                if (confChanged) {
                    logger.debug("Metrics configuration has changed and needs to be loaded.");
                    confChanged = false;
                    updateConf();
                }
                if (metricsChanged) {
                    logger.debug("Metrics have changed and need to be saved.");
                    metricsChanged = false;
                    writeMetrics();
                }
            }
        }
        catch (Exception ex) {
            logger.error("Error in metrics background thread", ex);
        }
    }

    protected void updateConf() {
        loadConf();
        for (BaseMetricImpl<?> metric : metrics.values()) {
            if (metric.confChanged) continue; // Pending from this side.
            updateEnabled(metric);
        }
    }

    protected void loadConf() {
        conf = fdbService.getDatabase().run(new Function<Transaction,Map<Tuple,byte[]>>() {
                                                @Override
                                                public Map<Tuple,byte[]> apply(Transaction tr) {
                                                    return readConf(tr);
                                                }
                                            });
        logger.debug("{}: {}", METRIC_CONF_KEY, conf);
    }

    protected Map<Tuple,byte[]> readConf(Transaction tr) {
        tr.options().setAccessSystemKeys();
        List<KeyValue> kvs = tr.getRange(Range.startsWith(metricKey(METRIC_CONF_KEY))).asList().get();
        Map<Tuple,byte[]> result = new HashMap<>();
        for (KeyValue kv : kvs) {
            byte[] tupleBytes = new byte[kv.getKey().length - PREFIX_BYTES.length];
            System.arraycopy(kv.getKey(), PREFIX_BYTES.length, tupleBytes, 0, tupleBytes.length);
            Tuple tuple = Tuple.fromBytes(tupleBytes);
            tuple = tuple.popFront();   // Always METRIC_CONF_KEY.
            result.put(tuple, kv.getValue());
        }
        tr.watch(metricKey(METRIC_CONF_CHANGES_KEY)).onReady(new Runnable() {
                @Override
                public void run() {
                    confChanged = true;
                    notifyBackground();
                }
            });
        return result;
    }

    protected static byte[] metricKey(Object... keys) {
        Tuple tuple = Tuple.from(keys);
        byte[] tupleBytes = tuple.pack();
        byte[] result = new byte[PREFIX_BYTES.length + tupleBytes.length];
        System.arraycopy(PREFIX_BYTES, 0, result, 0, PREFIX_BYTES.length);
        System.arraycopy(tupleBytes, 0, result, PREFIX_BYTES.length, tupleBytes.length);
        return result;
    }

    protected void updateEnabled(BaseMetricImpl<?> metric) {
        Tuple tuple = Tuple.from(metric.getType(), metric.getName(), address, "", 
                                 ENABLED_OPTION);
        byte[] enabled = conf.get(tuple);
        if (enabled == null) {
            metric.confChanged = true; // Write conf key for brand new metric.
            // Check wildcard enabling.
            tuple = Tuple.from(metric.getType(), metric.getName(), "", "", ENABLED_OPTION);
            enabled = conf.get(tuple);
            if (enabled != null) {
                setEnabled(metric, enabled[0] != 0, false);
            }
            metricsChanged = true;
            notifyBackground();
        }
        else {
            setEnabled(metric, enabled[0] != 0, false);
        }
    }

    protected void setEnabled(BaseMetricImpl<?> metric, boolean enabled, boolean explicit) {
        if (metric.enabled == enabled) return;
        metric.enabled = enabled;
        boolean notifyBackground = false;
        if (explicit) {
            metric.confChanged = true;
            metricsChanged = true;
            notifyBackground = true;
        }
        if (enabled) {
            synchronized (metric) {
                // As though just changed to current value.
                metricChanged(metric);
            }
            if (!anyEnabled) {
                anyEnabled = true;
                notifyBackground = true; // So goes to sleep instead of indefinite wait.
            }
        }
        if (notifyBackground) {
            notifyBackground();
        }
    }

    protected void writeMetrics() {
        boolean anyConfChanges = false;
        for (BaseMetricImpl<?> metric : metrics.values()) {
            if (!metric.confChanged && !metric.valueChanged) continue;
            synchronized (metric) {
                if (metric.confChanged) {
                    anyConfChanges = true;
                    pendingWrites.add(new KeyValue(metricKey(METRIC_CONF_KEY, metric.getType(), metric.getName(), address, "", ENABLED_OPTION),
                                                   metric.enabled ? ENABLED_TRUE : ENABLED_FALSE));
                    metric.confChanged = false;
                }
                if (metric.valueChanged) {
                    pendingWrites.add(new KeyValue(metricKey(METRIC_KEY, metric.getType(), metric.getName(), address, ""),
                                                   metric.encodeValue()));
                    metric.valueChanged = false;
                }
                for (int level = 0; level < NLEVELS; level++) {
                    MetricLevel<?> metricLevel = metric.levels.get(level);
                    while (true) {
                        MetricLevelValues values = metricLevel.values.pollFirst();
                        if (values == null) break;
                        pendingWrites.add(new KeyValue(metricKey(METRIC_KEY, metric.getType(), metric.getName(), address, "", level, values.start),
                                                       values.bytes));
                        // Continue to fill (will overwrite key with longer value).
                        if (metricLevel.values.isEmpty() && !values.isFull()) {
                            metricLevel.values.addLast(values);
                            break;
                        }
                    }
                }
            }
        }
        if (anyConfChanges) {
            byte[] bytes = new byte[16];
            random.nextBytes(bytes);
            pendingWrites.add(new KeyValue(metricKey(METRIC_CONF_CHANGES_KEY), bytes));
        }
        if (!pendingWrites.isEmpty()) {
            fdbService.getDatabase().run(new Function<Transaction,Void>() {
                                             @Override
                                             public Void apply(Transaction tr) {
                                                 tr.options().setAccessSystemKeys();
                                                 for (KeyValue kv : pendingWrites) {
                                                     tr.set(kv.getKey(), kv.getValue());
                                                 }
                                                 return null;
                                             }
                                         });
            pendingWrites.clear();
        }
    }

    protected <T> Future<List<FDBMetric.Value<T>>> readAllValues(Transaction tr, final BaseMetricImpl<T> metric) {
        tr.options().setAccessSystemKeys();
        return tr.getRange(metricKey(METRIC_KEY, metric.getType(), metric.getName(), address, "", 0),
                           metricKey(METRIC_KEY, metric.getType(), metric.getName(), address, "", NLEVELS))
            .asList()
            .map(new Function<List<KeyValue>,List<FDBMetric.Value<T>>> () {
                    @Override
                    public List<FDBMetric.Value<T>> apply(List<KeyValue> kvs) {
                        List<FDBMetric.Value<T>> result = new ArrayList<>();
                        for (KeyValue kv : kvs) {
                            result.addAll(metric.decodeValues(kv.getValue()));
                        }
                        // Merge all levels.
                        Collections.sort(result,
                                         new Comparator<FDBMetric.Value<T>> () {
                                             @Override
                                             public int compare(FDBMetric.Value<T> v1,
                                                                FDBMetric.Value<T> v2) {
                                                 if (v1.time < v2.time)
                                                     return -1;
                                                 else if (v1.time > v2.time)
                                                     return +1;
                                                 else
                                                     return 0;
                                             }
                                         });
                        return result;
                    }
                 });
    }

}
