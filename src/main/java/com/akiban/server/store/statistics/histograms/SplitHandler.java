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

package com.akiban.server.store.statistics.histograms;

import com.akiban.util.Equality;

import java.util.ArrayList;
import java.util.List;

abstract class SplitHandler<T> implements SampleVisitor<T> {

    protected abstract void handle(int segmentIndex, T input, int count);

    @Override
    public void init() {
        buffers = new ArrayList<SegmentBuffer<T>>(segments);
        for (int i = 0; i < segments; ++i) {
            buffers.add(new SegmentBuffer<T>());
        }
    }

    @Override
    public void finish() {
        checkInit();
        for (int i = 0; i < segments; ++i) {
            SegmentBuffer<T> buffer = buffers.get(i);
            T segment = buffer.last();
            int count = buffer.lastCount();
            if (count > 0)
                handle(i, segment, count);
        }
    }

    @Override
    public List<? extends T> visit(T input) {
        checkInit();
        List<? extends T> split = splitter.split(input);
        if (split.size() != segments)
            throw new IllegalStateException("required " + segments + ", found " + split.size() + ": " + split);
        recycleBin.clear();
        for (int i = 0; i < segments; ++i) {
            T segment = split.get(i);
            SegmentBuffer<T> buffer = buffers.get(i);
            T prev = buffer.last();
            int count = buffer.put(segment, recycleBin);
            if (count > 0) {
                handle(i, prev, count);
            }
        }
        return recycleBin;
    }

    public SplitHandler(Splitter<T> splitter) {
        this.splitter = splitter;
        this.segments = splitter.segments();
        if (segments < 1)
            throw new IllegalArgumentException("splitter must provide at least 1 segment: " + segments);
        this.recycleBin = new ArrayList<T>(segments);
    }

    private void checkInit() {
        if (buffers == null)
            throw new IllegalStateException("not initialized");
    }

    private final Splitter<T> splitter;
    private final int segments;
    private List<SegmentBuffer<T>> buffers;
    private final List<T> recycleBin;

    private static class SegmentBuffer<T> {
        /**
         * Adds an element to the stream. If that element is the same as the last element this buffer saw,
         * it won't be added to the stream, but will instead be recycled. If this element is different than the
         * one before, this class will return the number of times that other element had been seen. The caller
         * is responsible for having retrieved that element before calling this method.
         * @param segment the segment to add to the buffer
         * @param recycleBin where to put elements that need to be recycled
         * @return the number of times the last element appeared, or 0 if that's not known yet
         */
        int put(T segment, List<? super T> recycleBin) {
            int count;
            if (lastCount == 0) {
                // first element
                count = 0;
                lastCount = 1;
                last = segment;
            }
            else if (Equality.areEqual(last, segment)) {
                // same segment, just update lastCount
                ++lastCount;
                count = 0;
                recycleBin.add(segment);
            } else {
                // new segment. Return and reset lastCount, and reset last
                count = lastCount;
                lastCount = 1;
                last = segment;
            }
            return count;
        }

        T last() {

            return last;
        }
        
        int lastCount() {
            return lastCount;
        }

        @Override
        public String toString() {
            return (lastCount == 0)
                    ? "SegmentBuffer(last=" + last + ')'
                    : "SegmentBuffer(FRESH)";
        }

        T last;
        int lastCount = 0;
    }
}
