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

import com.akiban.util.AssertUtils;
import org.junit.Test;

import java.util.List;

import static com.akiban.server.store.histograms.BucketTestUtils.bucket;
import static com.akiban.util.CollectionUtils.list;

public final class BucketsTest {
    @Test
    public void noTies() {
        check(
                5,
                "a b c c d e e f g h h i i j k l l l m n".split(" "),
                bucketsList(
                        bucket("c", 2, 2, 2),
                        bucket("e", 2, 1, 1),
                        bucket("h", 2, 2, 2),
                        bucket("i", 2, 0, 0),
                        bucket("n", 1, 6, 4)
                )
        );
    }

    private <T extends Comparable<T>> void check(int maxBuckets, T[] inputs, List<Bucket<T>> expected) {
        List<Bucket<T>> actual = Buckets.compile(list(inputs), maxBuckets);
        AssertUtils.assertCollectionEquals("compiled buckets", expected, actual);
    }

    @SuppressWarnings("unchecked")
    private List<Bucket<String>> bucketsList(Bucket<?>... buckets) {
        return (List<Bucket<String>>) list(buckets);
        
    }
}
