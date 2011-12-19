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

package com.akiban.server.store.histograms;

import java.util.ArrayList;
import java.util.List;

final class Buckets<A extends Comparable<? super A>> {

    public static <A extends Comparable<A>> List<Bucket<A>> compile(int maxSize, Iterable<A> from) {
        BucketSource<A> source = new BucketSource<A>(from);
        Buckets<A> buckets = new Buckets<A>(maxSize);
        for (Bucket<A> bucket : source) {
            buckets.add(bucket);
        }
        return buckets.buckets();
    }

    public void add(Bucket<A> bucket) {
        // for all but the first entry, compare it to the previous entry
        if (sentinel.next != null && last.bucket.value().compareTo(bucket.value()) >= 1)
            throw new IllegalArgumentException("can't add " + bucket + " to " + buckets());
        BucketNode<A> node = nodeFor(bucket);
        node.prev = last;
        last.next = node;
        last = node;
        log("adding %s", bucket);
        log("%s", buckets());
        if (++size > maxSize) { // need to trim
            BucketNode<A> removeNode = nodeToRemove();
            // if the least popular node was the tail, we should fold it into its
            // prev. We can't do that, so instead, we'll fold its prev into it.
            if (removeNode.next == null)
                removeNode = removeNode.prev;   // to to
            Bucket<A> removeBucket = removeNode.bucket;

            Bucket<A> foldIntoBucket = removeNode.next.bucket;
            assert foldIntoBucket != null;
            foldIntoBucket.addLessThans(removeBucket.getEqualsCount() + removeBucket.getLessThanCount());
            foldIntoBucket.addLessThanDistincts(removeBucket.getLessThanDistinctsCount() + 1);
            // update the removeNode's prev and next to point to each other
            removeNode.prev.next = removeNode.next;
            if (removeNode.next != null)
                removeNode.next.prev = removeNode.prev;
            checkIntegrity();
        }
        log("%s", buckets());
        log("");
    }

    public List<Bucket<A>> buckets() {
        List<Bucket<A>> results = new ArrayList<Bucket<A>>(size);
        for(BucketNode<A> node = sentinel.next; node != null; node = node.next) {
            results.add(node.bucket);
        }
        return results;
    }

    private void checkIntegrity() {
        BucketNode<A> last = null;
        for(BucketNode<A> node = sentinel; node != null; node = node.next) {
            if (node.prev != last)
                System.out.printf("expected node.prev=%s but was %s%n", last, node.prev);
            last = node;
        }
    }

    public Buckets(int maxSize) {
        if (maxSize < 2)
            throw new IllegalArgumentException("max must be at least 2");
        this.maxSize = maxSize;
        this.sentinel = new BucketNode<A>();
        this.last = sentinel;
    }

    private BucketNode<A> nodeToRemove() {
        long lowestCount = Long.MAX_VALUE;
        List<BucketNode<A>> results = new ArrayList<BucketNode<A>>();
        for(BucketNode<A> node = sentinel.next; node != null; node = node.next) {
            long nodeEqs = node.bucket.getEqualsCount();
            if (nodeEqs == Long.MAX_VALUE)
                throw new IllegalStateException("node has too many counts: " + node);
            if (nodeEqs < lowestCount) {
                lowestCount = nodeEqs;
                results.clear();
                results.add(node);
            }
            else if (nodeEqs == lowestCount) {
                results.add(node);
            }
        }
        assert !results.isEmpty();
        BucketNode<A> result = results.get(0);
        log("removing %s (cost=%d) chosen from %d: %s", result, lowestCount, results.size(), results);
        return result;
    }

    private BucketNode<A> nodeFor(Bucket<A> bucket) {
        if (bucket == null)
            throw new IllegalArgumentException("bucket may not be null");
        return new BucketNode<A>(bucket);
    }

    private void log(String format, Object... args) {
        System.out.print("\t-- ");
        System.out.printf(format, args);
        System.out.println();
    }

    private final int maxSize;
    private int size;
    private final BucketNode<A> sentinel;
    private BucketNode<A> last;

    private static class BucketNode<A> {

        public BucketNode() {
            this(null);
        }

        public BucketNode(Bucket<A> bucket) {
            this.bucket = bucket;
        }

        @Override
        public String toString() {
            return (prev==null) ? "SENTINAL" : String.valueOf(bucket);
        }

        final Bucket<A> bucket;
        BucketNode<A> next;
        BucketNode<A> prev;
    }
}