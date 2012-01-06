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

package com.akiban.server.store.statistics.histograms;

import com.akiban.util.AssertUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.akiban.server.store.statistics.histograms.BucketTestUtils.bucket;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class BucketSamplerTest {

    @Test
    public void medianPointAtEnd() {
        check(
                4,
                "a b c   d e f   g h i   j k l",
                bucketsList(
                        bucket("c", 1, 2, 2),
                        bucket("f", 1, 2, 2),
                        bucket("i", 1, 2, 2),
                        bucket("l", 1, 2, 2)
                )
        );
    }

    @Test
    public void medianPointNotAtEnd() {
        check(
                4,
                "a b c   d e f   g h i   j k l   m n",
                bucketsList(
                        bucket("c", 1, 2, 2),
                        bucket("f", 1, 2, 2),
                        bucket("i", 1, 2, 2),
                        bucket("l", 1, 2, 2),
                        bucket("n", 1, 1, 1)
                )
        );
    }

    @Test
    public void spanStartsAfterMedianPointEndsOn() {
        check(
                4,
                "a b c   d d d   e f g   h i j",
                bucketsList(
                        bucket("c", 1, 2, 2),
                        bucket("d", 3, 0, 0),
                        bucket("g", 1, 2, 2),
                        bucket("j", 1, 2, 2)
                )
        );
    }

    @Test
    public void spanStartsOffMedianPointEndsOn() {
        check(
                4,
                "a b c   d e e   e e e   f g h",
                bucketsList(
                        bucket("c", 1, 2, 2),
                        bucket("e", 5, 1, 1),
                        bucket("h", 1, 2, 2)
                )
        );
    }

    @Test
    public void spanStartsOnMedianPointEndsOff() {
        check(
                4,
                "a b c   d d d   d e f   g h i",
                bucketsList(
                        bucket("c", 1, 2, 2),
                        bucket("d", 4, 0, 0),
                        bucket("f", 1, 1, 1),
                        bucket("i", 1, 2, 2)
                )
        );
    }

    @Test
    public void spanStartsOfMedianPointEndsOff() {
        check(
                4,
                "a b c   d e e   e e f   g h i",
                bucketsList(
                        bucket("c", 1, 2, 2),
                        bucket("e", 4, 1, 1),
                        bucket("f", 1, 0, 0),
                        bucket("i", 1, 2, 2)
                )
        );
    }

    @Test
    public void moreInputsThanBuckets() {
        check(
                40,
                "a b c d e f",
                bucketsList(
                        bucket("a", 1, 0, 0),
                        bucket("b", 1, 0, 0),
                        bucket("c", 1, 0, 0),
                        bucket("d", 1, 0, 0),
                        bucket("e", 1, 0, 0),
                        bucket("f", 1, 0, 0)
                )
        );
    }

    @Test
    public void asManyInputsThanBuckets() {
        check(
                6,
                "a b c d e f",
                bucketsList(
                        bucket("a", 1, 0, 0),
                        bucket("b", 1, 0, 0),
                        bucket("c", 1, 0, 0),
                        bucket("d", 1, 0, 0),
                        bucket("e", 1, 0, 0),
                        bucket("f", 1, 0, 0)
                )
        );
    }

    @Test
    public void moreSpansThanBuckets() {
        check(
                40,
                "a a b b c d d e f f",
                bucketsList(
                        bucket("a", 2, 0, 0),
                        bucket("b", 2, 0, 0),
                        bucket("c", 1, 0, 0),
                        bucket("d", 2, 0, 0),
                        bucket("e", 1, 0, 0),
                        bucket("f", 2, 0, 0)
                )
        );
    }

    @Test
    public void maxIsOne() {
        check(
                1,
                "a a b b c d d e f f",
                bucketsList(
                        bucket("f", 2, 8, 5)
                )
        );
    }

    @Test
    public void emptyInputs() {
        check(
                32,
                "",
                bucketsList()
        );
    }

    @Test
    public void testEqualityMean() {
        BucketSampler<String> sampler = runSampler(1, "a a a    b b  c c c c   d d d   e  f f f f f");
        assertEquals("mean equality", 3.0d, sampler.getEqualsMean(), 0.0);
    }

    @Test
    public void testEqualityStdDev() {
        BucketSampler<String> sampler = runSampler(1, "a a a    b b  c c c c   d d d   e  f f f f f ");
        assertEquals("equality std dev", 1.41421d, sampler.getEqualsStdDev(), 0.00001d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void maxIsZero() {
        new BucketSampler<String>(0, new MyLong(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void maxIsNegative() {
        new BucketSampler<String>(-1, new MyLong(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void expectedInputsIsNegative() {
        new BucketSampler<String>(16, new MyLong(-1));
    }
    
    @Test(expected = IllegalStateException.class)
    public void tooManyInputs() {
        BucketSampler<String> sampler = new BucketSampler<String>(16, new MyLong(0));
        Bucket<String> bucket = new Bucket<String>();
        bucket.init("alpha", 1);
        sampler.add(bucket);
    }

    @Test(expected = IllegalStateException.class)
    public void tooFewInputs() {
        BucketSampler<String> sampler = new BucketSampler<String>(16, new MyLong(1));
        sampler.buckets();
    }

    private BucketSampler<String> runSampler(int maxBuckets, String inputString) {
        String[] splits = inputString.length() == 0 ? new String[0] : inputString.split("\\s+");
        
        List<Bucket<String>> buckets = new ArrayList<Bucket<String>>();
        Bucket<String> lastBucket = null;
        for (String split : splits) {
            if (lastBucket == null || !split.equals(lastBucket.value())) {
                lastBucket = new Bucket<String>();
                lastBucket.init(split, 1);
                buckets.add(lastBucket);
            }
            else {
                lastBucket.addEquals();
            }
        }
        
        BucketSampler<String> sampler = new BucketSampler<String>(maxBuckets, new MyLong(splits.length));
        for (Bucket<String> bucket : buckets)
            sampler.add(bucket);
        return sampler;
    }
    
    private void check(int maxBuckets, String inputs, List<Bucket<String>> expected) {
        BucketSampler<String> sampler = runSampler(maxBuckets, inputs);
        AssertUtils.assertCollectionEquals("compiled buckets", expected, sampler.buckets());
    }

    private List<Bucket<String>> bucketsList(Bucket<?>... buckets) {
        List<Bucket<String>> list = new ArrayList<Bucket<String>>();
        for (Bucket<?> bucket : buckets) {
            @SuppressWarnings("unchecked")
            Bucket<String> cast = (Bucket<String>) bucket;
            list.add(cast);
        }
        return list;
    }
}
