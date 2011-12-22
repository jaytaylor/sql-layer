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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.akiban.server.store.histograms.BucketTestUtils.bucket;
import static com.akiban.util.CollectionUtils.list;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class BucketSourceTest {

    @Test
    public void consolidateRepeats() {
        check(
                list("a a b b c c".split(" ")),
                buckets(bucket("a", 2), bucket("b", 2), bucket("c", 2))
        );
    }

    @Test
    public void noRepeats() {
        check(
                list("a b c".split(" ")),
                buckets(bucket("a", 1), bucket("b", 1), bucket("c", 1))
        );
    }

    @Test
    public void outOfOrderRepeats() {
        check(
                list("a b b a".split(" ")),
                buckets(bucket("a", 1), bucket("b", 2), bucket("a", 1))
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void noRemoves() {
        List<String> list = new ArrayList<String>(list("a b c".split(" "))); // list's iterator supports remove
        BucketSource<String> source = new BucketSource<String>(BucketTestUtils.expandList(list), 1, 3);
        Iterator<List<Bucket<String>>> iterator = source.iterator();
        assertEquals("first bucket", bucket("a", 1), extractAndRecycle(iterator, source));
        UnsupportedOperationException uoe = null;
        try {
            iterator.remove();
        }
        catch (UnsupportedOperationException e) {
            uoe = e;
        }
        assertEquals("second bucket", bucket("b", 1), extractAndRecycle(iterator, source));
        assertEquals("third bucket", bucket("c", 1), extractAndRecycle(iterator, source));
        assertFalse("iter should have been done", iterator.hasNext());
        // doing it this way instead of just assertNotNull helps the @Test annotation document expected behavior
        if (uoe != null)
            throw uoe;
    }
    
    private Bucket<String> extractAndRecycle(Iterator<List<Bucket<String>>> iterator, BucketSource<String> source) {
        List<Bucket<String>> next = iterator.next();
        Bucket<String> result = BucketTestUtils.extractSingle(next);
        source.release(next);
        return result;
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullInputs() {
        new BucketSource<Object>(null, 5, 9);
    }

    private <T> void check(Iterable<T> inputs, List<? extends Bucket<?>> expected) {
        BucketSource<T> bucketSource = new BucketSource<T>(BucketTestUtils.expandList(inputs), 1, Integer.MAX_VALUE);
        List<Bucket<T>> actual = new ArrayList<Bucket<T>>();
        for (List<Bucket<T>> buckets : bucketSource) {
            actual.add(BucketTestUtils.extractSingle(buckets));
            bucketSource.release(buckets);
        }
        assertEquals("buckets", expected, actual);
    }

    private static List<? extends Bucket<?>> buckets(Bucket<?>... buckets) { // needed for varargs generics safety
        return list(buckets);
    }
}
