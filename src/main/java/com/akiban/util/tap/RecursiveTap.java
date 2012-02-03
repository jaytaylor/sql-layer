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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

abstract class RecursiveTap extends Tap
{
    // Object interface

    public final String toString()
    {
        return String.format("%s inCount=%,d outCount=%,d time=%,dms",
                             name, inCount, outCount, cumulativeNanos / MILLION);
    }

    // Tap interface

    public final void in()
    {
        long now = System.nanoTime();
        Stack<RecursiveTap> tapStack = tapStack();
        if (tapStack != null) {
            inCount++;
            inNanos = now;
            if (!tapStack.empty()) {
                RecursiveTap current = tapStack.peek();
                current.lastDuration = now - current.inNanos;
                current.cumulativeNanos += current.lastDuration;
            }
            tapStack.push(this);
        }
        // else: outermost tap has just been disabled
    }

    public final void out()
    {
        long now = System.nanoTime();
        outCount++;
        endNanos = now;
        lastDuration = now - inNanos;
        cumulativeNanos += lastDuration;
        Stack<RecursiveTap> tapStack = tapStack();
        if (tapStack != null) {
            if (!tapStack.empty()) {
                RecursiveTap newCurrent = tapStack.pop();
                newCurrent.inNanos = now;
            }
        }
        // else: outermost tap has just been disabled
    }

    public final long getDuration()
    {
        return lastDuration;
    }

    public void reset()
    {
        inCount = 0;
        outCount = 0;
        cumulativeNanos = 0;
    }

    public final void appendReport(StringBuilder buffer)
    {
        buffer.append(String.format("%20s inCount=%,10d outCount=%,10d time=%,12dms",
                                name, inCount, outCount, cumulativeNanos / 1000000));
        if (outCount > 0) {
            buffer.append(String.format("  per=%,12dns  interval=%,12dns",
                                    cumulativeNanos / outCount, (endNanos - startNanos) / outCount));
        }
    }

    // RecursiveTap interface

    public RecursiveTap createSubsidiaryTimer(String name)
    {
        if (!(this instanceof Outermost)) {
            throw new IllegalArgumentException("Only the topmost recursive timer may have a subsidiary timer");
        }
        return new Subsidiary(name, (Outermost) this);
    }

    public static RecursiveTap createRecursiveTap(String name)
    {
        return new Outermost(name);
    }

    // For use by subclasses

    // tapStack() returns the current stack of taps, which may be null. The callers (RecursiveTap.in/out)
    // should call tapStack() exactly once. This insulates in/out processing from a stack "disappearing".
    protected abstract Stack<RecursiveTap> tapStack();

    protected RecursiveTap(String name)
    {
        super(name);
    }

    // Class state

    private static final int MILLION = 1000000;

    // Object state

    protected volatile long cumulativeNanos = 0;
    protected volatile long inNanos = Long.MIN_VALUE;
    protected volatile long startNanos = System.nanoTime();
    protected volatile long endNanos = System.nanoTime();
    protected volatile long lastDuration = Long.MIN_VALUE;

    static class Outermost extends RecursiveTap
    {
        // Tap interface

        @Override
        public void reset()
        {
            super.reset();
            tapStack = new Stack<RecursiveTap>();
        }

        @Override
        public void disable()
        {
            tapStack = null;
        }

        @Override
        public TapReport[] getReports()
        {
            TapReport[] reports = new TapReport[subsidiaryTaps.size() + 1];
            int r = 0;
            for (Subsidiary tap : subsidiaryTaps) {
                reports[r++] = new TapReport(tap.getName(), tap.inCount, tap.outCount, tap.cumulativeNanos);
            }
            reports[r] = new TapReport(name, inCount, outCount, cumulativeNanos);
            return reports;
        }

        // RecursiveTap interface

        @Override
        protected Stack<RecursiveTap> tapStack()
        {
            return tapStack;
        }

        public Outermost(String name)
        {
            super(name);
        }

        private volatile Stack<RecursiveTap> tapStack;
        private final List<Subsidiary> subsidiaryTaps = new ArrayList<Subsidiary>();
    }

    static class Subsidiary extends RecursiveTap
    {
        @Override
        public TapReport[] getReports()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Stack<RecursiveTap> tapStack()
        {
            return outermostTap.tapStack();
        }
        
        Subsidiary(String name, Outermost outermostTap)
        {
            super(name);
            this.outermostTap = outermostTap;
            outermostTap.subsidiaryTaps.add(this);
        }

        private RecursiveTap outermostTap;
    }
}
