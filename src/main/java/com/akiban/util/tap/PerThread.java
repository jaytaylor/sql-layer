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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tap implementation that aggregates information on a per-thread basis. An
 * instance of this class maintains a collection of subordinate Taps, one
 * per thread. These are created on demand as new threads invoke
 * {@link #in()} or {@link #out()}.
 * <p/>
 * By default each subordinate Tap is a {@link TimeAndCount}, but the
 * two-argument constructor provides a way to override that default.
 */
class PerThread extends Tap
{
    // Object interface
    
    @Override
    public String toString()
    {
        return "PerThread(" + tapClass.getName() + ")";
    }
    
    // Tap interface

    @Override
    public void in()
    {
        threadLocal.get().in();
    }

    @Override
    public void out()
    {
        threadLocal.get().out();
    }

    @Override
    public long getDuration()
    {
        return threadLocal.get().getDuration();
    }

    @Override
    public void reset()
    {
        for (Tap tap : threadMap.values()) {
            tap.reset();
        }
    }

    @Override
    public void appendReport(StringBuilder sb)
    {
        Map<Thread, Tap> threadMap = new TreeMap<Thread, Tap>(THREAD_COMPARATOR);
        threadMap.putAll(this.threadMap);
        // TODO - fix this: thread names not necessarily unique. putAll could lose threads!
        assert this.threadMap.size() == threadMap.size();
        for (Map.Entry<Thread, Tap> entry : threadMap.entrySet()) {
            sb.append(NEW_LINE);
            sb.append("==");
            sb.append(entry.getKey().getName());
            sb.append("==");
            sb.append(NEW_LINE);
            entry.getValue().appendReport(sb);
        }
    }

    @Override
    public TapReport getReport()
    {
        long inCount = 0;
        long outCount = 0;
        long elapsedTime = 0;
        for (Tap tap : threadMap.values()) {
            TapReport threadTapReport = tap.getReport();
            inCount += threadTapReport.getInCount();
            outCount += threadTapReport.getOutCount();
            elapsedTime += threadTapReport.getCumulativeTime();
        }
        return new PerThreadTapReport(name, inCount, outCount, elapsedTime, threadMap);
    }
    
    // PerThread interface

    /**
     * Create a PerThread instance the adds a new instance of the supplied
     * subclass of {@link com.akiban.util.tap.Tap} for each Thread. Note: the class
     * {@link com.akiban.util.tap.PerThread} may not itself be added.
     *
     * @param name  Name of the Tap
     * @param tapClass Class from which new Tap instances are created.
     */
    public PerThread(String name, Class<? extends Tap> tapClass)
    {
        super(name);
        if (tapClass == this.getClass()) {
            throw new IllegalArgumentException(String.format("May not add a %s to %s", tapClass.getName(), this));
        }
        this.tapClass = tapClass;
    }

    // Class state

    private static final Comparator<Thread> THREAD_COMPARATOR = new Comparator<Thread>()
    {
        @Override
        public int compare(Thread o1, Thread o2)
        {
            return o1.getName().compareTo(o2.getName());
        }
    };

    // Object state

    private final Class<? extends Tap> tapClass;
    private final Map<Thread, Tap> threadMap = new ConcurrentHashMap<Thread, Tap>();
    private final ThreadLocal<Tap> threadLocal = new ThreadLocal<Tap>()
    {
        @Override
        protected Tap initialValue()
        {
            Tap tap;
            try {
                tap = tapClass.getConstructor(String.class).newInstance(name);
                tap.reset();
            } catch (Exception e) {
                tap = new Null(name);
                LOG.warn("Unable to create tap of class " + tapClass.getSimpleName(), e);
            }
            threadMap.put(Thread.currentThread(), tap);
            return tap;
        }
    };

    // Inner classes

    // TODO: Why is this class needed? reportMap is never used, so could use a TapReport instead.

    public static class PerThreadTapReport extends TapReport
    {
        PerThreadTapReport(String name, long inCount, long outCount, long elapsedTime, Map<Thread, Tap> threadMap)
        {
            super(name, inCount, outCount, elapsedTime);
            for (Map.Entry<Thread, Tap> entry : threadMap.entrySet()) {
                this.reportMap.put(
                    entry.getKey().getName(),
                    entry.getValue().getReport());
            }
        }

        private final Map<String, TapReport> reportMap = new HashMap<String, TapReport>();
    }
}
