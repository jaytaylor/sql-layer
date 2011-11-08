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

package com.akiban.util;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loosely inspired by SystemTap, this class implements a generic mechanism for
 * timing and counting executions of code sections. Unlike SystemTap, you must
 * add explicit method calls to your code to delimit measurement start and end
 * points. However, HotSpot appears to optimize disabled instances of these
 * method calls away, so there is little or no cost in the added calls other
 * than noise in the source code.
 * <p />
 * Application code should do something like the following:
 * 
 * <pre>
 *   // As a static initializer
 *   private final static Tap MY_TAP = Tap.add("myTap");
 *   ...
 *      // Within a code body
 *   	MY_TAP.in();
 *   	...
 *   	... The code section to measure
 *   	...
 *   	MY_TAP.out();
 * </pre>
 * <p />
 * The static {@link #add(Tap)} method creates an instance of an initially
 * disabled {@link Tap.Dispatch}. when enabled, this {@link Tap.Dispatch} will
 * invoke a {@link Tap.TimeAndCount} instance to count and time the invocations
 * of {@link #in()} and {@link #out()}. You may instantiate other Tap subclasses
 * with the {@link #add(Tap)} method, for example,
 * 
 * <pre>
 * private final static Tap COUNT_ONLY_TAP = Tap.add(new Tap.Count(&quot;counter&quot;));
 * </pre>
 * 
 * <p />
 * When needed, call {@link #setEnabled(String, boolean)} to turn on/off
 * performance monitoring for one or more Dispatch objects by name. The String
 * argument is a regular expression to be applied to the name; for example
 * <code>Tap.setEnabled("my.*", true)</code> will enable monitoring of the code
 * section in the example above.
 * <p />
 * The results are available in String or object form. Call {@link #report()} to
 * get a printable string representation of the results. Note that for
 * convenience when using jconsole, the {@link #report()} method also dumps the
 * report string to the log at INFO level. Use MY_TAP.getReport() to get a
 * {@link TapReport} object.
 * <p />
 * Note that the TimeAndCount subclass of {@link Tap} uses
 * {@link System#nanoTime()} to measure elapsed time. The System#nanoTime() call
 * itself takes several hundred nanoseconds, so don't attempt to use this class
 * to measure very short code segments.
 * <p />
 * The {@link #in()} and {@link #out()} methods are meant to be paired, but it
 * is not necessary to implement a try/finally block to guarantee that
 * <tt>out</tt> is called after <tt>in</tt>. Generally this makes the tool
 * easier to use, but it means that if you instrument a body of code that
 * sometimes or always throws an Exception, those instances will not be timed.
 * You can detect this issue in the most of the Tap subclasses by comparing the
 * reported <tt>inCount</tt> and <tt>outCount</tt> values.
 * <p />
 * You may implement a custom subclass of {@link Tap} to provide extended
 * behavior. To enable it, call {@link #setCustomTap(String, Class)} as follows:
 * 
 * <pre>
 * Tap.setCustomTap(&quot;myTap&quot;, MyTapSubclass.class);
 * </pre>
 * 
 * <p />
 * Currently the following Tap subclasses are defined:
 * <dl>
 * <dt>{@link Tap.TimeAndCount}</dt>
 * <dd>Count and measure the elapsed time between each pair of calls to the
 * {@link #in()} and {@link #out()} methods.</dd>
 * <dt>{@link Tap.Count}</dt>
 * <dd>Simply count the number of alls to {@link #in()} and {@link #out()}.
 * Faster because {@link System#nanoTime()} is not called</dd>
 * <dt>{@link Tap.TimeStampLog}</dt>
 * <dd>Like {@link Tap.TimeAndCount} but in addition, maintains a log of
 * relative times at which {@link #in()} and {@link #out()} have been called.
 * These are relative to a starting wall-clock time that is accessible from the
 * {@link TapReport} object returned by {@link #getReport()}</dd>
 * <dt>{@link Tap.PerThread}</dt>
 * <dd>Sub-dispatches each {@link #in()} and {@link #out()} call to a
 * subordinate {@link Tap} on private to the current Thread. Results are
 * reported by thread name.</dd>
 * </dl>
 * {@link Tap.PerThread} requires another Tap subclass to dispatch to, as shown
 * here:
 * 
 * <pre>
 *   // As a static initializer
 *   private final static Tap ANOTHER_TAP = 
 *   	Tap.add(new PerThread("anotherTap",  Tap.TimeStampLog.class));
 *   ...
 *      // Within a multi-threaded code body
 *   	ANOTHER_TAP.in();
 *   	...
 *   	... The code section to measure
 *   	...
 *   	ANOTHER_TAP.out();
 *      ...
 *      // To see a formatted text report in the log.
 * </pre>
 * 
 * In this example, a {@link Tap.Count} instance will be created for each thread
 * that calls either {@link Tap#in()} or {@link Tap#out()}.
 * 
 * @author peter
 * 
 */
public abstract class Tap {

    public static class PointTap {

        public void hit() {
            internal.in();
            internal.out();
        }

        private PointTap(Tap internal) {
            this.internal = internal;
        }

        private final Tap internal;
    }

    public static class InOutTap {

        public void in() {
            internal.in();
        }

        public void out() {
            internal.out();
        }

        /**
         * Reset the tap.
         * @deprecated using this method indicates an improper separation of concerns between defining events (which
         * is what this class does) and reporting on them.
         */
        @Deprecated
        public void reset() {
            internal.reset();
        }

        /**
         * Gets the duration of the tap's in-to-out timer.
         * @deprecated using this method indicates an improper separation of concerns between defining events (which
         * is what this class does) and reporting on them.
         * @return the tap's duration
         */
        @Deprecated
        public long getDuration() {
            return internal.getDuration();
        }

        /**
         * Gets the tap's report
         * @deprecated using this method indicates an improper separation of concerns between defining events (which
         * is what this class does) and reporting on them.
         * @return the underlying tap's report
         */
        @Deprecated
        public TapReport getReport() {
            return internal.getReport();
        }

        private InOutTap(Tap internal) {
            this.internal = internal;
        }

        private final Tap internal;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Tap.class.getName());

    public final static String NEW_LINE = System.getProperty("line.separator");

    private static Map<String, Dispatch> dispatches = new TreeMap<String, Dispatch>();

    private static boolean registered;

    public static PointTap createCount(String name) {
        return new PointTap(add(new PerThread(name, Count.class)));
    }
    
    public static PointTap createCount(String name, boolean enabled) {
    	PointTap ret =  new PointTap(add(new PerThread(name, Count.class)));
        Tap.setEnabled(name, enabled);
    	return ret;
    }

    public static InOutTap createTimer(String name) {
        return new InOutTap(add(new PerThread(name, TimeAndCount.class)));
    }

    public static InOutTap createTimeStampLog(String name) {
        return new InOutTap(add(new PerThread(name, TimeStampLog.class)));
    }

    /**
     * Add or replace a new Dispatch declaration. Generally called by static
     * initializers of classes to be measured.
     * 
     * @param tap
     *            The Tap to call when enabled.
     * @return The Dispatch instance.
     */
    private static Dispatch add(final Tap tap) {
        final Dispatch dispatch = new Dispatch(tap.getName(), tap);
        dispatches.put(tap.getName(), dispatch);
        return dispatch;
    }

    /**
     * Re-initialize counters and timers for selected {@link Tap.Dispatch}es.
     * 
     * @param regExPattern
     *            regular expression for names of Dispatches to reset.
     */
    public static void setEnabled(final String regExPattern, final boolean on) {
        final Pattern pattern = Pattern.compile(regExPattern);
        for (final Dispatch tap : dispatches.values()) {
            if (pattern.matcher(tap.getName()).matches()) {
                tap.setEnabled(on);
            }
        }
    }

    /**
     * Re-initialize counters and timers for selected {@link Tap.Dispatch}es.
     * 
     * @param regExPattern
     *            regular expression for names of Dispatches to reset.
     */
    public static void reset(final String regExPattern) {
        final Pattern pattern = Pattern.compile(regExPattern);
        for (final Dispatch tap : dispatches.values()) {
            if (pattern.matcher(tap.getName()).matches()) {
                tap.reset();
            }
        }
    }

    /**
     * Fetch an array of current report statuses of {@link Tap} selected by
     * name.
     * 
     * @param regExPattern
     *            regular expression for names of Dispatches to report
     * @return array of TapReport objects.
     */
    public static TapReport[] getReport(final String regExPattern) {
        final List<TapReport> reports = new ArrayList<TapReport>();
        final Pattern pattern = Pattern.compile(regExPattern);
        for (final Dispatch tap : dispatches.values()) {
            if (pattern.matcher(tap.getName()).matches()) {
                final TapReport report = tap.getReport();
                if (report != null) {
                    reports.add(report);
                }
            }
        }
        return reports.toArray(new TapReport[reports.size()]);
    }

    /**
     * Add a custom {@link Tap} subclass. Specify a regExPattern to select from
     * previously installed {@link Tap.Dispatch} instances, plus a {@link Class}
     * defining a custom Tap implementation.
     * 
     * @param regExPattern
     *            regular expression to select names
     * @param clazz
     *            Subclass of {@link Tap} to enable
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws IllegalArgumentException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public static void setCustomTap(final String regExPattern,
            final Class<? extends Tap> clazz) throws SecurityException,
            NoSuchMethodException, IllegalArgumentException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException {
        final Pattern pattern = Pattern.compile(regExPattern);

        final Constructor<? extends Tap> constructor = clazz
                .getConstructor(new Class[] { String.class });
        for (final Dispatch dispatch : dispatches.values()) {
            if (pattern.matcher(dispatch.getName()).matches()) {
                final Tap tap = (Tap) constructor
                        .newInstance(new Object[] { dispatch.getName() });
                dispatch.enabledTap = tap;
            }
        }
    }

    /**
     * Return a String containing a formatted report generated by traversing and
     * appending results from all registered Taps. This method also logs the
     * report in a human-readable format at INFO level.
     * 
     * @return Formated report
     */
    public static String report() {
        final StringBuilder sb = new StringBuilder();
        for (final Tap tap : dispatches.values()) {
            int length = sb.length();
            tap.appendReport(sb);
            if (sb.length() > length) {
                sb.append(NEW_LINE);
            }
        }
        final String result = sb.toString();
        LOG.info("Tap Report" + NEW_LINE + NEW_LINE + result + NEW_LINE);
        return result;
    }

    /**
     * Register an MXBean to make methods of this class available remotely from
     * JConsole or other JMX client. Does nothing if there already is a
     * registered MXBean.
     * @throws NullPointerException 
     * @throws MalformedObjectNameException 
     * @throws NotCompliantMBeanException 
     * @throws MBeanRegistrationException 
     * @throws InstanceAlreadyExistsException 
     * 
     * @throws Exception
     */
    public synchronized static void registerMXBean() throws MalformedObjectNameException, NullPointerException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        if (!registered) {
            ObjectName mxbeanName = new ObjectName("com.akiban:type=Tap");
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(new TapMXBeanImpl(), mxbeanName);
            registered = true;
        }
    }

    /**
     * Unregister the MXBean created by {@link #registerMXBean()}. Does nothing
     * if there is no registered MXBean.
     * @throws NullPointerException 
     * @throws MalformedObjectNameException 
     * @throws InstanceNotFoundException 
     * @throws MBeanRegistrationException 
     * 
     * @throws Exception
     */
    public synchronized static void unregisterMXBean() throws MalformedObjectNameException, NullPointerException, MBeanRegistrationException, InstanceNotFoundException {
        if (registered) {
            ObjectName mxbeanName = new ObjectName("com.akiban:type=Tap");
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(mxbeanName);
            registered = false;
        }
    }

    protected final String name;

    /**
     * Base constructor. Every Tap requires a name.
     * 
     * @param name
     */
    private Tap(final String name) {
        this.name = name;
    }

    /**
     * @return the name of the Tap
     */
    public String getName() {
        return name;
    }

    /**
     * Mark the beginning of a section of code to be timed. Package-private; should be invoked by PointTap or InOutTap.
     */
    abstract void in();

    /**
     * Mark the end of a section of code to be timed. Package-private; should be invoked by PointTap or InOutTap.
     */
    abstract void out();
    
    /**
     * @return duration of time spent in section of code to be timed
     */
    public abstract long getDuration();

    /**
     * Reset any accumulated statistics to zero.
     */
    public abstract void reset();

    /**
     * Append text to the supplied {@link StringBuilder} containing the current
     * accumulated statistics for this Tap. The Null Tap should do nothing.
     * 
     * @param sb
     */
    public abstract void appendReport(final StringBuilder sb);

    /**
     * Return a {@link TapReport} object containing the accumulated statistics
     * for this Tap. The Null Tap should return null.
     * 
     * @return A Result object or <tt>null</tt>
     */
    public abstract TapReport getReport();

    /**
     * A {@link Tap} Implementation that simply dispatches to another Tap
     * instance. Used to hold the "switch" that determines whether a Null,
     * TimeAndCount or other kind of Tap is invoked.
     * <p />
     * Reason for this dispatch mechanism is that HotSpot seems to be able to
     * optimize out a dispatch to an instance of Null. Therefore code can invoke
     * the methods of a Dispatch object set to Null with low or no performance
     * penalty.
     */
    private static class Dispatch extends Tap {

        protected Tap currentTap;

        protected Tap enabledTap;

        public Dispatch(final String name, final Tap tap) {
            super(name);
            this.currentTap = new Null(name);
            this.enabledTap = tap;
        }

        public final void setEnabled(final boolean on) {
            currentTap = on ? enabledTap : new Null(name);
        }

        public void in() {
            currentTap.in();
        }

        public void out() {
            currentTap.out();
        }
        
        public long getDuration() {
            return currentTap.getDuration();
        }

        public void reset() {
            currentTap.reset();
        }

        public String getName() {
            return name;
        }

        public void appendReport(final StringBuilder sb) {
            currentTap.appendReport(sb);
        }

        public TapReport getReport() {
            return currentTap.getReport();
        }

        public String toString() {
            return currentTap.toString();
        }
    }

    /**
     * A {@link Tap} subclass that does nothing. This is the initial target of
     * every added {@link Tap.Dispatch}.
     * 
     */
    private static class Null extends Tap {

        public Null(final String name) {
            super(name);
        }

        public void in() {
            // do nothing
        }

        public void out() {
            // do nothing
        }
        
        public long getDuration() {
            return 0;
        }

        public void reset() {
            // do nothing
        }

        public void appendReport(final StringBuilder sb) {
            // do nothing;
        }

        public String toString() {
            return "NullTap(" + name + ")";
        }

        public TapReport getReport() {
            return null;
        }
    }

    /**
     * A Tap subclass that counts calls to {@link #in()} and {@link #out()}.
     * Generally this is faster than {@link TimeAndCount} because the system
     * clock is not read.
     */
    private static class Count extends Tap {

        public Count(final String name) {
            super(name);
        }

        long inCount = 0;

        long outCount = 0;

        public void in() {
            inCount++;
        }

        public void out() {
            outCount++;
        }
        
        public long getDuration() {
            return 0;
        }

        public void reset() {
            inCount = 0;
            outCount = 0;
        }

        public void appendReport(final StringBuilder sb) {
            sb.append(String.format("%20s inCount=%,10d outCount=%,10d", name,
                    inCount, outCount));
        }

        public String toString() {
            return String.format("%s inCount=%,d outCount=%,d", name, inCount,
                    outCount);
        }

        public TapReport getReport() {
            return new TapReport(getName(), inCount, outCount, 0);
        }
    }

    /**
     * A Tap subclass that counts and times the intervals between calls to
     * {@link #in()} and {@link #out()}.
     */
    private static class TimeAndCount extends Tap {

        public TimeAndCount(final String name) {
            super(name);
        }

        long cumulativeNanos = 0;
        long inCount = 0;
        long outCount = 0;
        long inNanos = Long.MIN_VALUE;
        long startNanos = System.nanoTime();
        long endNanos = System.nanoTime();
        long lastDuration = Long.MIN_VALUE;

        public void in() {
            inCount++;
            inNanos = System.nanoTime();
        }

        public void out() {
            if (inNanos != Long.MIN_VALUE) {
                long now = System.nanoTime();
                endNanos = now;
                lastDuration = now - inNanos;
                cumulativeNanos += lastDuration;
                outCount++;
                inNanos = Long.MIN_VALUE;
            }
        }
        
        public long getDuration() {
            return lastDuration;
        }

        public void reset() {
            inCount = 0;
            outCount = 0;
            cumulativeNanos = 0;
            inNanos = Long.MIN_VALUE;
        }

        public void appendReport(final StringBuilder sb) {
            sb.append(String.format(
                    "%20s inCount=%,10d outCount=%,10d time=%,12dms", name,
                    inCount, outCount, cumulativeNanos / 1000000));
            if (outCount > 0) {
                sb.append(String.format("  per=%,12dns  interval=%,12dns",
                        cumulativeNanos / outCount, (endNanos - startNanos)
                                / outCount));
            }
        }

        public String toString() {
            return String.format("%s inCount=%,d outCount=%,d time=%,dms",
                    name, inCount, outCount, cumulativeNanos / 1000000);
        }

        public TapReport getReport() {
            return new TapReport(getName(), inCount, outCount, cumulativeNanos);
        }

    }

    /**
     * A Tap subclass that counts and times the intervals between calls to
     * {@link #in()} and {@link #out()}. In addition, it keeps a log of in/out
     * times. The log is an array of long values arranged in pairs alternating
     * between {@link #in()} and {@link #out()} times. Each time value is
     * measured in nanoseconds since the first call to {@link #in()} since the
     * Tap was created or since {@link #reset()} was last called.
     * <p />
     * The {@link TapReport} returned by the {@link #getReport()} method of this
     * class contains all the timing details, including the timestamp array and
     * the wall-clock time (@link {@link System#currentTimeMillis()}
     * corresponding with the zero timestamp value.
     * 
     */
    static class TimeStampLog extends Tap {

        public final static int LOG_SIZE_DELTA = 100000;

        public final static int MAX_LOG_SIZE = 10000000; // 10 million entries

        public TimeStampLog(final String name) {
            super(name);
        }

        long startMillis = Long.MIN_VALUE;
        long startNanos = Long.MIN_VALUE;
        long endNanos = Long.MAX_VALUE;
        long cumulativeNanos = 0;
        long inCount = 0;
        long outCount = 0;
        long inNanos = Long.MIN_VALUE;
        long lastDuration = Long.MIN_VALUE;

        long[] log = new long[0];

        public void in() {
            inCount++;
            inNanos = System.nanoTime();
            if (startNanos == Long.MIN_VALUE) {
                startMillis = System.currentTimeMillis();
                startNanos = inNanos;
            }
        }

        public void out() {
            if (inNanos != Long.MIN_VALUE) {
                long now = System.nanoTime();
                endNanos = now;
                lastDuration = now - inNanos;
                cumulativeNanos += lastDuration;
                if (outCount * 2 < MAX_LOG_SIZE) {
                    if (outCount * 2 >= log.length) {
                        try {
                            final long[] longerLog = new long[log.length
                                    + LOG_SIZE_DELTA];
                            System.arraycopy(log, 0, longerLog, 0, log.length);
                            log = longerLog;
                        } catch (OutOfMemoryError oome) {
                            // don't try to do anything here.
                        }
                    }
                    if (outCount * 2 + 2 < log.length) {
                        log[(int) outCount * 2] = inNanos - startNanos;
                        log[(int) outCount * 2 + 1] = now - startNanos;
                    }
                }
                outCount++;
                inNanos = Long.MIN_VALUE;
            }
        }
        
        public long getDuration() {
            return lastDuration;
        }

        public void reset() {
            inCount = 0;
            outCount = 0;
            cumulativeNanos = 0;
            inNanos = Long.MIN_VALUE;
            startMillis = Long.MIN_VALUE;
            startNanos = Long.MIN_VALUE;
            log = new long[0];
        }

        public void appendReport(final StringBuilder sb) {
            sb.append(String.format(
                    "%20s inCount=%,10d outCount=%,10d time=%,12dms", name,
                    inCount, outCount, cumulativeNanos / 1000000));
            if (outCount > 0) {
                sb.append(String.format("  per=%,12dns  interval=%,12dns",
                        cumulativeNanos / outCount, (endNanos - startNanos)
                                / outCount));
            }
        }

        public String toString() {
            return String.format("%s inCount=%,d outCount=%,d time=%,dms",
                    name, inCount, outCount, cumulativeNanos / 1000000);
        }

        public TapReport getReport() {
            return new TimeStampTapReport(getName(), inCount, outCount,
                    cumulativeNanos, startMillis, log);
        }

        /**
         * TapReport subclass that includes the start time of the Timestamp log
         * from {@link System#currentTimeMillis()} and the timestamp log itself
         * as an array of alternating {@link Tap#in()} and {@link Tap#out()}
         * times. Each value represents the number of nanoseconds elapsed since
         * the zero time.
         * 
         * 
         */
        public static class TimeStampTapReport extends TapReport {
            private final long[] log;

            private final long startMillis;

            private TimeStampTapReport(final String name, final long inCount,
                    final long outCount, final long cumulativeTime,
                    final long startMillis, final long[] log) {
                super(name, inCount, outCount, cumulativeTime);
                this.log = new long[Math.min(log.length, (int) outCount * 2)];
                System.arraycopy(log, 0, this.log, 0, this.log.length);
                this.startMillis = startMillis;
            }

            /**
             * @return The wall-clock time in millisceconds of timestamp 0.
             */
            public long getStartTimeMillis() {
                return startMillis;
            }

            /**
             * @return The timestamp log
             */
            public long[] getLog() {
                return log;
            }
        }

    }

    /**
     * Tap implementation that aggregates information on a per-thread basis. An
     * instance of this class maintains a collection of subordinate Taps, one
     * per thread. These are created on demand as new threads invoke
     * {@link #in()} or {@link #out()}.
     * <p />
     * By default each subordinate Tap is a {@link TimeAndCount}, but the
     * two-argument constructor provides a way to override that default.
     * 
     */
    static class PerThread extends Tap {

        private static final Comparator<Thread> THREAD_COMPARATOR = new Comparator<Thread>() {
            @Override
            public int compare(Thread o1, Thread o2) {
                return o1.getName().compareTo(o2.getName());
            }
        };
        private final ThreadLocal<Tap> threadLocal = new ThreadLocal<Tap>() {
            @Override
            protected Tap initialValue() {
                Tap tap;
                try {
                    tap = clazz.getConstructor(String.class).newInstance(name);
                } catch (Exception e) {
                    tap = new Null(name);
                    LOG.warn("Unable to create tap of class " + clazz.getSimpleName(), e);
                }
                threadMap.put(Thread.currentThread(), tap);
                return tap;
            }
        };

        private final Map<Thread, Tap> threadMap = new ConcurrentHashMap<Thread, Tap>();

        private final Class<? extends Tap> clazz;

        /**
         * {@link TapReport} subclass that contains map of subordinate TapReport
         * instances, one per thread. The map key is threadName for simple
         * correlation with
         * 
         * @author peter
         * 
         */
        public static class PerThreadTapReport extends TapReport {

            private final Map<String, TapReport> reportMap = new HashMap<String, TapReport>();

            PerThreadTapReport(final String name, final long inCount,
                    final long outCount, final long elapsedTime,
                    Map<Thread, Tap> threadMap) {
                super(name, inCount, outCount, elapsedTime);
                for (final Map.Entry<Thread, Tap> entry : threadMap.entrySet()) {
                    this.reportMap.put(
                            entry.getKey().getName(),
                            entry.getValue().getReport()
                    );
                }
            }

            public Map<String, TapReport> getTapReportMap() {
                return new TreeMap<String, TapReport>(reportMap);
            }
        }

        /**
         * Create a PerThread instance the adds a new instance of the supplied
         * subclass of {@link Tap} for each Thread. Note: the class
         * {@link PerThread} may not itself be added.
         * 
         * @param name
         *            Name of the Tap
         * @param clazz
         *            Class from which new Tap instances are created.
         */
        public PerThread(final String name, final Class<? extends Tap> clazz) {
            super(name);
            if (clazz == this.getClass()) {
                throw new IllegalArgumentException("May not add a "
                        + clazz.getName() + " to " + this);
            }
            this.clazz = clazz;
        }

        @Override
        public void appendReport(StringBuilder sb) {
            final Map<Thread, Tap> threadMap = new TreeMap<Thread, Tap>(THREAD_COMPARATOR);
            threadMap.putAll(this.threadMap);
            for (final Map.Entry<Thread, Tap> entry : threadMap.entrySet()) {
                sb.append(NEW_LINE);
                sb.append("==");
                sb.append(entry.getKey().getName());
                sb.append("==");
                sb.append(NEW_LINE);
                entry.getValue().appendReport(sb);
            }
        }

        @Override
        public TapReport getReport() {
            long inCount = 0;
            long outCount = 0;
            long elapsedTime = 0;
            for (final Tap tap : threadMap.values()) {
                final TapReport threadTapReport = tap.getReport();
                inCount += threadTapReport.getInCount();
                outCount += threadTapReport.getOutCount();
                elapsedTime += threadTapReport.getCumulativeTime();
            }
            return new PerThreadTapReport(name, inCount, outCount, elapsedTime, threadMap);
        }

        @Override
        public void in() {
            final Tap tap = getTap();
            tap.in();
        }

        @Override
        public void out() {
            final Tap tap = getTap();
            tap.out();
        }
        
        @Override
        public long getDuration() {
            final Tap tap = getTap();
            return tap.getDuration();
        }

        @Override
        public void reset() {
            for (final Tap tap : threadMap.values()) {
                tap.reset();
            }
        }

        @Override
        public String toString() {
            return "PerThread(" + clazz.getName() + ")";
        }

        private Tap getTap() {
            return threadLocal.get();
        }
    }

    /**
     * A micro-benchmark to prove that a Dispatch to Null gets optimized away.
     * Actually, the results are strange: the loop containing the added in/out
     * calls seems to run a bit faster than the loop without those calls. We
     * think this is an artifact of micro-benchmarking.
     * 
     * @param args
     */
    public static void main(final String[] args) {
        final Dispatch t1 = add(new TimeAndCount("t1"));
        final Dispatch t2 = add(new TimeAndCount("t2"));
        t1.setEnabled(true);

        long z = 0;

        long count = 1000 * 1000 * 1000;

        {
            System.out.print("Loop " + count + " times, Null Tap invoked: ");
            t1.in();
            long t = 0;
            for (long i = 0; i < count; i++) {
                t2.in();
                t = t + i;
                if (t > count) {
                    t = t / 2;
                }
                t2.out();
            }
            t1.out();
            System.out.println("  " + t1.toString());
            t1.reset();
            z += t;
        }

        {
            System.out.print("Loop " + count + " times, no Tap");
            t1.in();
            long t = 0;
            for (long i = 0; i < count; i++) {
                t = t + i;
                if (t > count) {
                    t = t / 2;
                }
            }
            t1.out();
            System.out.println("  " + t1.toString());
            t1.reset();
            z += t;
        }

        count = count / 100;

        {
            System.out.print("Loop " + count
                    + " times, TimeAndCount Tap invoked: ");
            t2.setEnabled(true);
            t1.in();
            long t = 0;
            for (long i = 0; i < count; i++) {
                t2.in();
                t = t + i;
                if (t > count) {
                    t = t / 2;
                }
                t2.out();
            }
            t1.out();
            System.out.println("  " + t1.toString());
            t1.reset();
            z += t;
        }

        System.out.println("Report:");
        System.out.println(report());

        System.out.println("Printing a computed result to make sure "
                + "HotSpot doesn't optimize the whole computation away: " + z);

    }
}
