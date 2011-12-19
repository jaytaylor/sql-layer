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
import java.util.List;

import static com.akiban.server.store.histograms.BucketTestUtils.bucket;
import static com.akiban.util.CollectionUtils.list;
import static org.junit.Assert.assertEquals;

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

    @Test(expected = IllegalArgumentException.class)
    public void nullInputs() {
        new BucketSource<Object>(null);
    }

    private <T> void check(Iterable<T> inputs, List<? extends Bucket<?>> expected) {
        BucketSource<T> bucketSource = new BucketSource<T>(inputs);
        List<Bucket<T>> actual = new ArrayList<Bucket<T>>();
        for (Bucket<T> bucket : bucketSource) {
            actual.add(bucket);
        }
        assertEquals("buckets", expected, actual);
    }

    private static List<? extends Bucket<?>> buckets(Bucket<?>... buckets) { // needed for varargs generics safety
        return list(buckets);
    }
}
