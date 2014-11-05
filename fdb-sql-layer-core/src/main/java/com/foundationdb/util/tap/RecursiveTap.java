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

package com.foundationdb.util.tap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

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
        Deque<RecursiveTap> tapStack = tapStack();
        if (tapStack != null) {
            // print("in %s", this);
            long now = System.nanoTime();
            inCount++;
            inNanos = now;
            if (!tapStack.isEmpty()) {
                RecursiveTap current = tapStack.peek();
                current.lastDuration = now - current.inNanos;
                current.cumulativeNanos += current.lastDuration;
                // print("    added %s to %s", current.lastDuration / MILLION, current);
            }
            tapStack.push(this);
        }
        // else: outermost tap has just been disabled
    }

    public final void out()
    {
        Deque<RecursiveTap> tapStack = tapStack();
        if (tapStack != null) {
            // print("out %s", this);
            if (!tapStack.isEmpty()) {
                long now = System.nanoTime();
                outCount++;
                endNanos = now;
                lastDuration = now - inNanos;
                cumulativeNanos += lastDuration;
                // print("    added %s to %s", lastDuration / MILLION, this);
                RecursiveTap current = tapStack.pop();
                if (current == this) {
                    if (!tapStack.isEmpty()) {
                        RecursiveTap newCurrent = tapStack.peek();
                        newCurrent.inNanos = now;
                    }
                }
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

    public void appendReport(String label, StringBuilder buffer)
    {
        buffer.append(String.format("%s %20s inCount=%,10d outCount=%,10d time=%,12dms",
                                    label, name, inCount, outCount, cumulativeNanos / 1000000));
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

    // For use by subclasses

    // tapStack() returns the current stack of taps, which may be null. The callers (RecursiveTap.in/out)
    // should call tapStack() exactly once. This insulates in/out processing from a stack "disappearing".
    protected abstract Deque<RecursiveTap> tapStack();

    protected RecursiveTap(String name)
    {
        super(name);
    }
    
    // For use by this class
    
    private void print(String template, Object ... args)
    {
        System.out.println(String.format(template, args));
    }

    // Class state

    private static final int MILLION = 1000000;

    // Object state

    protected volatile long cumulativeNanos;
    protected volatile long inNanos;
    protected volatile long startNanos = System.nanoTime();
    protected volatile long endNanos;
    protected long lastDuration;

    static class Outermost extends RecursiveTap
    {
        // Tap interface

        @Override
        public void reset()
        {
            tapStack = null;
            super.reset();
            for (Subsidiary subsidiaryTap : subsidiaryTaps) {
                subsidiaryTap.reset();
            }
            tapStack = new ArrayDeque<>();
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

        @Override
        public final void appendReport(String label, StringBuilder buffer)
        {
            for (Subsidiary tap : subsidiaryTaps) {
                tap.appendReport(label, buffer);
            }
        }

        // RecursiveTap interface

        @Override
        protected Deque<RecursiveTap> tapStack()
        {
            return tapStack;
        }

        public Outermost(String name)
        {
            super(name);
        }

        private volatile Deque<RecursiveTap> tapStack;
        private final List<Subsidiary> subsidiaryTaps = new ArrayList<>();
    }

    static class Subsidiary extends RecursiveTap
    {
        @Override
        public TapReport[] getReports()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Deque<RecursiveTap> tapStack()
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
