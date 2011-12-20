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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

final class Buckets<T extends Comparable<? super T>> {

    public static <T extends Comparable<T>> List<Bucket<T>> compile(Iterable<? extends T> from, int maxSize) {
        return compile(from, new Buckets<T>(maxSize));
    }

    // intended for testing
    static <T extends Comparable<? super T>>
    List<Bucket<T>> compile(Iterable<? extends T> from, Buckets<T> usingBuckets) {
        BucketSource<T> source = new BucketSource<T>(from);
        for (Bucket<T> bucket : source) {
            usingBuckets.add(bucket);
        }
        return usingBuckets.buckets();
    }

    public void add(Bucket<T> bucket) {
        // for all but the first entry, compare it to the previous entry
        if (sentinel.next != null && last.bucket.value().compareTo(bucket.value()) >= 1)
            throw new IllegalArgumentException("can't add " + bucket + " to " + buckets());
        BucketNode<T> node = nodeFor(bucket);
        node.prev = last;
        last.next = node;
        last = node;
        if (++size > maxSize) { // need to trim
            BucketNode<T> removeNode = nodeToRemove();
            // if the least popular node was the tail, we should fold it into its
            // prev. We can't do that, so instead, we'll fold its prev into it.
            if (removeNode.next == null)
                removeNode = removeNode.prev;    TODO this breaks the map!
            Bucket<T> removeBucket = removeNode.bucket;

            Bucket<T> foldIntoBucket = removeNode.next.bucket;
            assert foldIntoBucket != null;
            foldIntoBucket.addLessThans(removeBucket.getEqualsCount() + removeBucket.getLessThanCount());
            foldIntoBucket.addLessThanDistincts(removeBucket.getLessThanDistinctsCount() + 1);
            // update the removeNode's prev and next to point to each other
            removeNode.prev.next = removeNode.next;
            if (removeNode.next != null)
                removeNode.next.prev = removeNode.prev;
            --size;
            checkIntegrity();
        }
        else {
            addLastToBucketNodeSets();
        }
    }

    public List<Bucket<T>> buckets() {
        List<Bucket<T>> results = new ArrayList<Bucket<T>>(size);
        for(BucketNode<T> node = sentinel.next; node != null; node = node.next) {
            results.add(node.bucket);
        }
        return results;
    }

    private void checkIntegrity() {
        BucketNode<T> last = null;
        for(BucketNode<T> node = sentinel; node != null; node = node.next) {
            if (node.prev != last)
                System.out.printf("expected node.prev=%s but was %s%n", last, node.prev);
            last = node;
        }
        if (buckets().size() != size) {
            System.out.println("buckets too big!");
        }
    }

    public Buckets(int maxSize) {
        if (maxSize < 2)
            throw new IllegalArgumentException("max must be at least 2");
        this.maxSize = maxSize;
        this.sentinel = new BucketNode<T>();
        this.last = sentinel;
        this.bucketNodeSets = new TreeMap<Long, BucketNodeSet<T>>();
    }

    private BucketNode<T> nodeToRemove() {
        long lastNodePopularity = last.bucket.getEqualsCount();
        Map.Entry<Long,BucketNodeSet<T>> leastPopularEntry = bucketNodeSets.firstEntry();
        long leastPopular = leastPopularEntry.getKey();
        if (lastNodePopularity < leastPopular) {
            return last;
        }
        if (lastNodePopularity == leastPopular) {
            BucketNodeSet<T> bucketNodeSet = leastPopularEntry.getValue();
            // if there have been more elements seen than we see here, then each element has had
            // a (t-N)/t chance of being picked, where N = current list.size and t = total seen.
            // Multiply both num and denom by 1/(t-N) and you get a 1 / (t/(t-N)) chance.
            if (bucketNodeSet.totalSeen > bucketNodeSet.bucketNodes.size()) {
                float totalFloat = bucketNodeSet.totalSeen;
                totalFloat /= bucketNodeSet.battles;
//                float denom =totalFloat- ((float)bucketNodeSet.bucketNodes.size());
//                int randN = Math.round(totalFloat/denom);
                
                int randN = Math.round(bucketNodeSet.totalSeen - bucketNodeSet.bucketNodes.size());
                if (0 == rand(randN)) {
                    bucketNodeSet.totalSeen++;
                    return last;
                }
                else {
                    bucketNodeSet.add(last);
                }
            }
            else {
                bucketNodeSet.add(last);
            }
        }
        else {
            // not the least popular! Add this to its entry, creating if needed
            addLastToBucketNodeSets();
        }
        // now, clear a random one out of the least popular entry
        BucketNodeSet<T> bucketNodeSet = leastPopularEntry.getValue();
        final BucketNode<T> result;
        if (bucketNodeSet.bucketNodes.size() == 1) {
            // last element! get it and remove this node set from the map.
            // this is safe to do because the final results are guaranteed to have only
            // nodes which are more popular than this one.
            result = bucketNodeSet.bucketNodes.get(0);
            bucketNodeSets.remove(leastPopular);
        }
        else {
            List<BucketNode<T>> list = bucketNodeSet.bucketNodes;
            result = list.remove(rand(list.size()));
            bucketNodeSet.battles++;
        }
        return result;
    }

    private void addLastToBucketNodeSets() {
        long lastPopularity = last.bucket.getEqualsCount();
        BucketNodeSet<T> bucketNodeSet = bucketNodeSets.get(lastPopularity);
        if (bucketNodeSet == null) {
            bucketNodeSet = new BucketNodeSet<T>();
            bucketNodeSets.put(lastPopularity, bucketNodeSet);
        }
        bucketNodeSet.add(last);
    }

    private BucketNode<T> nodeFor(Bucket<T> bucket) {
        if (bucket == null)
            throw new IllegalArgumentException("bucket may not be null");
        return new BucketNode<T>(bucket);
    }
    
    private int rand(int n) {
        if (random == null)
            random = new Random(System.nanoTime());
        return random.nextInt(n);
    }

    private final int maxSize;
    private int size;
    private final BucketNode<T> sentinel;
    private final NavigableMap<Long,BucketNodeSet<T>> bucketNodeSets;
    private Random random;
    private BucketNode<T> last;

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
    
    private static class BucketNodeSet<T> {
        
        private void add(BucketNode<T> node) {
            bucketNodes.add(node);
            ++totalSeen;
        }
        
        private final List<BucketNode<T>> bucketNodes = new ArrayList<BucketNode<T>>();
        private int totalSeen = 0;
        private int battles = 0;
    }
}