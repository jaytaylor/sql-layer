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

package com.foundationdb.server.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * COPIED FROM com.persistit.util.ThreadSequencer
 *
 * <p>
 * Internal utility that allows tests to define execution sequences to confirm
 * specific concurrent execution patterns. Subject code incorporates calls to
 * the static method {@link #sequence(int)}. Each usage in the application code
 * should follow this pattern:
 * </p>
 * <ul>
 * <li>In the {@link SequencerConstants} interface, add a static variable with a
 * name denoting the place in code where sequencing will occur as follows:
 * 
 * <pre>
 * <code>
 *   final static int SOME_LOCATION = ThreadSequence.allocate("SOME_LOCATION");
 * </code>
 * </pre>
 * 
 * </li>
 * <li>In the class being tested, add a static import to ThreadSequence.* and
 * then call itself:
 * 
 * <pre>
 * <code>
 *      sequence(SOME_LOCATION);
 * </code>
 * </pre>
 * 
 * </li>
 * </ul>
 * <p>
 * The {@link #allocate(String)} method simply allocates a unique integer and
 * stores an associated location name. Currently the maximum number of allocated
 * locations is 64.
 * </p>
 * <p>
 * The {@link #sequence(int)} method blocks on a Semaphore until a condition for
 * release is recognized. The condition is determined by a schedule that the
 * test class must define.
 * </p>
 * <p>
 * The entire ThreadSequence mechanism must be enabled via
 * {@link #enableSequencer(boolean)}. By default all calls to
 * {@link #sequence(int)} invoke an empty method in the NullSequencer subclass,
 * which is fast.
 * </p>
 * <p>
 * In conjunction with enabling the sequencer, a test must also add one or more
 * sequencing schedules. Each schedule denotes two sets: the "join set" and the
 * "release set". When a thread calls sequence(x), the thread adds location x to
 * a list of blocked locations. It then determines whether that set covers any
 * join set, and if so, it releases all threads in the associated release set.
 * For example, suppose there are three location A, B and C. Consider two
 * sections of code executed by two different threads: <code><pre>
 *   sequence(A)
 *   // do this before B
 *   sequence(C)
 * </pre></code> and <code><pre>
 *   sequence(B)
 *   // do this after A
 * </pre></code> A suitable schedule might include the
 * pairs (A, B)->(A) and (B, C)->(B, C). In this example one thread blocks until
 * both sequence(A) and sequence(B) have been called. Then the schedule (A,
 * B)->(A) releases the call to sequence(A) so that the first thread runs. When
 * the first thread calls sequence(C), the schedule (B,C)->(B,C) runs, releasing
 * the second thread's call to sequence(B) and allowing the first thread to
 * continue without blocking.
 * </p>
 * <p>
 * Follow existing usage in {@link SequencerConstants} for defining schedules.
 * Test classes should invoke the static method {@link #addSchedules(int[][])}
 * to add schedules defined in SequencerConstants.
 * </p>
 * <p>
 * Note: a malformed schedule can cause threads blocked within the sequence
 * method to remain blocked forever.
 * </p>
 * 
 * @author peter
 * 
 */
public class ThreadSequencer implements SequencerConstants {

    private final static DisabledSequencer DISABLED_SEQUENCER = new DisabledSequencer();

    private final static EnabledSequencer ENABLED_SEQUENCER = new EnabledSequencer();

    private static volatile Sequencer _sequencer = DISABLED_SEQUENCER;

    private final static List<String> LOCATIONS = new ArrayList<>();

    private final static int MAX_LOCATIONS = 64;

    public synchronized static int allocate(final String locationName) {
        for (final String alreadyRegistered : LOCATIONS) {
            assert !alreadyRegistered.equals(locationName) : "Location name " + locationName + " is already in use";
        }
        int value = LOCATIONS.size();
        assert value < MAX_LOCATIONS : "Too many ThreadSequence locations";
        LOCATIONS.add(locationName);
        return value;
    }

    /**
     * Possibly block (only when ThreadSequencer is enabled for tests) until a
     * scheduling condition is met. This method is called from strategically
     * selected places in the main code to enable concurrency tests. See above
     * for details.
     * 
     * @param location
     *            integer uniquely identifying a location in code
     */
    public static void sequence(final int location) {
        _sequencer.sequence(location);
    }

    /**
     * Enable sequencer debugging.
     * 
     * @param history
     *            indicates whether to maintain a schedule history
     */
    public static void enableSequencer(final boolean history) {
        ENABLED_SEQUENCER.clear();
        if (history) {
            ENABLED_SEQUENCER.enableHistory();
        }
        _sequencer = ENABLED_SEQUENCER;
    }

    /**
     * Disable sequencer debugging.
     */
    public static void disableSequencer() {
        _sequencer = DISABLED_SEQUENCER;
        ENABLED_SEQUENCER.clear();
    }

    public static void addSchedule(final int[] awaitLocations, final int[] releaseLocations) {
        ENABLED_SEQUENCER.addSchedule(bits(awaitLocations), bits(releaseLocations));
    }

    public static void addSchedules(final int[][] pairs) {
        for (int index = 0; index < pairs.length; index += 2) {
            addSchedule(pairs[index], pairs[index + 1]);
        }
    }

    public static String sequencerHistory() {
        return ENABLED_SEQUENCER.history();
    }

    public static int[] rawSequenceHistoryCopy() {
        return ENABLED_SEQUENCER.rawHistoryCopy();
    }

    public static void appendHistoryElement(StringBuilder sb, int location) {
        if (location < MAX_LOCATIONS) {
            sb.append('+');
            sb.append(LOCATIONS.get(location));
        } else if ((location = out(location)) < MAX_LOCATIONS) {
            sb.append('-');
            sb.append(LOCATIONS.get(location));
        }
    }

    public static String describeHistory(int[] history) {
        StringBuilder sb = new StringBuilder();
        if (history != null) {
            for (Integer location : history) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                appendHistoryElement(sb, location);
            }
        }
        return sb.toString();
    }

    public static String describePartialOrdering(int[]... args) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < args.length; ++i) {
            if (i != 0) {
                builder.append(',');
            }
            builder.append('{');
            for (int location : args[i]) {
                appendHistoryElement(builder, location);
            }
            builder.append('}');
        }
        return builder.toString();
    }

    /**
     * Compare a particular sequence history to a collection of required
     * subsets. That is, compare the total ordering of the history as specified
     * by the given ranges where the elements within a given range can be in any
     * order.
     * <p>
     * For example, a <code>partialOrderings</code> of:
     * <ul>
     * <li>
     * 
     * <pre>
     * { { A_IN, B_IN }, { OUT_B }, { IN_C } }
     * </pre>
     * 
     * </li>
     * </ul>
     * Could be satisfied by either of these <code>history</code>:
     * <ul>
     * <li>
     * 
     * <pre>
     * { A_IN, B_IN, OUT_B, IN_C }
     * </pre>
     * 
     * </li>
     * <li>
     * 
     * <pre>
     * { B_IN, A_IN, OUT_B, IN_C }
     * </pre>
     * 
     * </li>
     * </ul>
     * But not by any that contains IN_C before OUT_B.
     * </p>
     * <p>
     * <b>Note:</b> Both parameters will be modified by sorting.
     * </p>
     * 
     * @param history
     *            An ordered history, e.g. from
     *            {@link #rawSequenceHistoryCopy()}.
     * @param partialOrderings
     *            Total ordering specification of unordered subsets
     * 
     * @return <code>true</code> if the history fulfilled the required
     *         orderings.
     */
    public static boolean historyMeetsPartialOrdering(int[] history, int[]... partialOrderings) {
        /*
         * Sort each subset and equivalent ranges in the actual history. Then a
         * simple element wise comparison.
         */
        int offset = 0;
        for (int[] subset : partialOrderings) {
            Arrays.sort(subset);
            int nextOffset = offset + subset.length;
            if (nextOffset > history.length) {
                return false;
            }
            Arrays.sort(history, offset, nextOffset);
            for (int i = 0; i < subset.length; ++i) {
                if (subset[i] != history[offset + i]) {
                    return false;
                }
            }
            offset = nextOffset;
        }
        return true;
    }

    public static int[] array(int... args) {
        return args;
    }

    public static int out(int location) {
        return Integer.MAX_VALUE - location;
    }

    private static long bits(final int[] locations) {
        long bits = 0;
        for (final int location : locations) {
            assert location >= 0 && location < MAX_LOCATIONS : "Location must be between 0 and 63, inclusive";
            bits |= (1L << location);
        }

        return bits;
    }

    interface Sequencer {
        /**
         * A location is a long with one bit set that denotes one of
         * MAX_LOCATIONS possible locations in code where a join point can
         * occur.
         * 
         * @param location
         */
        public void sequence(final int location);

        /**
         * Clear the schedule
         */
        public void clear();

        /**
         * Add an element to the schedule. The arguments each represent a set of
         * locations in code. A schedule element affects the behavior of the
         * {@link #sequence(int)} method. When a thread calls
         * sequence(location), that thread blocks until the set of all currently
         * blocked threads covers the await field of one of the schedule
         * elements. Once the await field is covered, then the threads waiting
         * at locations denoted by the release field of that schedule element
         * are allowed to continue.
         * 
         * @param await
         *            bit map of locations required to meet schedule
         * @param release
         *            bit map of locations to release after schedule is met
         */
        public void addSchedule(final long await, final long release);
    }

    private static class DisabledSequencer implements Sequencer {

        @Override
        public void sequence(int location) {
        }

        @Override
        public void clear() {
        }

        @Override
        public void addSchedule(long await, long release) {
        }
    }

    private static class EnabledSequencer implements Sequencer {
        private final List<Long> _schedule = new ArrayList<>();
        private final Semaphore[] _semaphores = new Semaphore[MAX_LOCATIONS];
        private long _waiting = 0;
        private long _enabled = 0;
        private int[] _waitingCount = new int[MAX_LOCATIONS];
        private List<Integer> _history;

        {
            for (int index = 0; index < _semaphores.length; index++) {
                _semaphores[index] = new Semaphore(0);
            }
        }

        @Override
        public void sequence(int location) {
            assert location >= 0 && location < MAX_LOCATIONS : "Location must be between 0 and 63, inclusive";
            Semaphore semaphore = null;

            synchronized (this) {
                if ((_enabled & (1L << location)) == 0) {
                    return;
                }

                _waiting |= (1L << location);
                _waitingCount[location]++;
                semaphore = _semaphores[location];
                long release = 0;
                for (int index = 0; index < _schedule.size(); index += 2) {
                    long await = _schedule.get(index);
                    if ((_waiting & await) == await) {
                        release = _schedule.get(index + 1);
                        break;
                    }
                }
                for (int index = 0; index < MAX_LOCATIONS; index++) {
                    if ((release & (1L << index)) != 0) {
                        if (location == index) {
                            semaphore = null;
                        } else {
                            _semaphores[index].release();
                        }
                    }
                }
                if (_history != null) {
                    _history.add(location);
                }
            }

            if (semaphore != null) {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            synchronized (this) {
                if (_history != null) {
                    _history.add(out(location));
                }
                if (--_waitingCount[location] == 0) {
                    _waiting &= ~(1L << location);
                }
            }
        }

        @Override
        public synchronized void clear() {
            _schedule.clear();
            _waiting = 0;
            _enabled = 0;
            _history = null;
            for (int index = 0; index < _semaphores.length; index++) {
                _semaphores[index].release(Integer.MAX_VALUE);
                _semaphores[index] = new Semaphore(0);
            }
        }

        @Override
        public synchronized void addSchedule(long await, long release) {
            for (int index = 0; index < _schedule.size(); index += 2) {
                long current = _schedule.get(index);
                assert (current & await) != current : "Schedules may not overlap";
                assert (current & await) != await : "Schedules may not overlap";
                assert (await & release) != 0 : "No thread is released";
            }
            _schedule.add(await);
            _schedule.add(release);
            _enabled |= release;
        }

        private void enableHistory() {
            _history = new ArrayList<>();
        }

        public synchronized String history() {
            String desc = "";
            if (_history != null) {
                int[] copy = rawHistoryCopy();
                desc = describeHistory(copy);
                _history.clear();
            }
            return desc;
        }

        public synchronized int[] rawHistoryCopy() {
            int[] historyCopy = null;
            if (_history != null) {
                historyCopy = new int[_history.size()];
                for (int i = 0; i < _history.size(); ++i) {
                    historyCopy[i] = _history.get(i);
                }
            }
            return historyCopy;
        }
    }
}
