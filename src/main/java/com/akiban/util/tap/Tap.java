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

package com.akiban.util.tap;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
 * disabled {@link Dispatch}. when enabled, this {@link Dispatch} will
 * invoke a {@link TimeAndCount} instance to count and time the invocations
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
 * <dt>{@link TimeAndCount}</dt>
 * <dd>Count and measure the elapsed time between each pair of calls to the
 * {@link #in()} and {@link #out()} methods.</dd>
 * <dt>{@link Count}</dt>
 * <dd>Simply count the number of alls to {@link #in()} and {@link #out()}.
 * Faster because {@link System#nanoTime()} is not called</dd>
 * <dt>{@link PerThread}</dt>
 * <dd>Sub-dispatches each {@link #in()} and {@link #out()} call to a
 * subordinate {@link Tap} on private to the current Thread. Results are
 * reported by thread name.</dd>
 * </dl>
 * {@link PerThread} requires another Tap subclass to dispatch to, as shown
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
 * In this example, a {@link Count} instance will be created for each thread
 * that calls either {@link Tap#in()} or {@link Tap#out()}.
 * 
 * @author peter
 * 
 */
public abstract class Tap {

    static final Logger LOG = LoggerFactory.getLogger(Tap.class.getName());

    static final String NEW_LINE = System.getProperty("line.separator");

    private static final Map<String, Dispatch> dispatches = new TreeMap<String, Dispatch>();

    private static String DEFAULT_ON_PROPERTY = "taps_default_on";

    private static boolean registered;

    protected final String name;

    public static PointTap createCount(String name) {
        return createCount(name, defaultOn());
    }
    
    public static PointTap createCount(String name, boolean enabled) {
        PointTap ret =  new PointTap(add(new PerThread(name, Count.class)));
        Tap.setEnabled(name, enabled);
        return ret;
    }

    public static InOutTap createTimer(String name) {
        return createTimer(name, defaultOn());
    }

    public static InOutTap createTimer(String name, boolean enabled) {
        InOutTap ret = new InOutTap(add(new PerThread(name, TimeAndCount.class)));
        Tap.setEnabled(name, enabled);
        return ret;
    }

    public static void defaultToOn(boolean defaultIsOn) {
        System.getProperties().setProperty(DEFAULT_ON_PROPERTY, Boolean.toString(defaultIsOn));
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
        synchronized (dispatches) {
            dispatches.put(tap.getName(), dispatch);
        }
        return dispatch;
    }
    
    private static boolean defaultOn() {
        return Boolean.getBoolean(DEFAULT_ON_PROPERTY);
    }

    /**
     * Re-initialize counters and timers for selected {@link Dispatch}es.
     * 
     * @param regExPattern
     *            regular expression for names of Dispatches to reset.
     */
    public static void setEnabled(final String regExPattern, final boolean on) {
        final Pattern pattern = Pattern.compile(regExPattern);
        for (final Dispatch tap : dispatchesCopy()) {
            if (pattern.matcher(tap.getName()).matches()) {
                tap.setEnabled(on);
            }
        }
    }

    /**
     * Re-initialize counters and timers for selected {@link Dispatch}es.
     * 
     * @param regExPattern
     *            regular expression for names of Dispatches to reset.
     */
    public static void reset(final String regExPattern) {
        final Pattern pattern = Pattern.compile(regExPattern);
        for (final Dispatch tap : dispatchesCopy()) {
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
        for (final Dispatch tap : dispatchesCopy()) {
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
     * previously installed {@link Dispatch} instances, plus a {@link Class}
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
        for (final Dispatch dispatch : dispatchesCopy()) {
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
        for (final Tap tap : dispatchesCopy()) {
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

    private static Collection<Dispatch> dispatchesCopy()
    {
        synchronized (dispatches) {
            return new ArrayList<Dispatch>(dispatches.values());
        }
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

    /**
     * Base constructor. Every Tap requires a name.
     * 
     * @param name
     */
    Tap(final String name) {
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
