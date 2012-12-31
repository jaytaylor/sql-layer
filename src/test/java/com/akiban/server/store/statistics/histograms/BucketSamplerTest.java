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

import com.akiban.util.AssertUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.akiban.server.store.statistics.histograms.BucketTestUtils.bucket;
import static org.junit.Assert.assertEquals;

public final class BucketSamplerTest {

    @Test
    public void medianPointAtEnd() {
        check(
                4,
                "a   b c d   e f g h   i j k l",
                bucketsList(
                        bucket("a", 1, 0, 0),
                        bucket("d", 1, 2, 2),
                        bucket("h", 1, 3, 3),
                        bucket("l", 1, 3, 3)
                )
        );
    }

    @Test
    public void medianPointNotEvenlyDistributed() {
        check(
                5,
                "a   b c d   e f g   h i j k   l m n",
                bucketsList(
                        bucket("a", 1, 0, 0),
                        bucket("d", 1, 2, 2),
                        bucket("g", 1, 2, 2),
                        bucket("k", 1, 3, 3),
                        bucket("n", 1, 2, 2)
                )
        );
    }

    @Test
    public void spanStartsAfterMedianPointEndsOn() {
        check(
                5,
                "a   b c   d d d   e f g   h i j",
                bucketsList(
                        bucket("a", 1, 0, 0),
                        bucket("c", 1, 1, 1),
                        bucket("d", 3, 0, 0),
                        bucket("g", 1, 2, 2),
                        bucket("j", 1, 2, 2)
                )
        );
    }

    @Test
    public void spanStartsOffMedianPointEndsOn() {
        check(
                5,
                "a   b c   d e e   e e e   f g h",
                bucketsList(
                        bucket("a", 1, 0, 0),
                        bucket("c", 1, 1, 1),
                        bucket("e", 5, 1, 1),
                        bucket("h", 1, 2, 2)
                )
        );
    }

    @Test
    public void spanStartsOnMedianPointEndsOff() {
        check(
                5,
                "a   b c   d d d   d e f   g h i",
                bucketsList(
                        bucket("a", 1, 0, 0),
                        bucket("c", 1, 1, 1),
                        bucket("d", 4, 0, 0),
                        bucket("f", 1, 1, 1),
                        bucket("i", 1, 2, 2)
                )
        );
    }

    @Test
    public void spanStartsOfMedianPointEndsOff() {
        check(
                5,
                "a   b c   d e e   e e f   g h i",
                bucketsList(
                        bucket("a", 1, 0, 0),
                        bucket("c", 1, 1, 1),
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
    public void maxIsTwo() {
        // The practical minimum number of buckets is 2, one for the min value, one for everything else.
        check(
                2,
                "a a b b c d d e f f",
                bucketsList(
                        bucket("a", 2, 0, 0),
                        bucket("f", 2, 6, 4)
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
    public void appendingKeepsSamplingUnchanged() {
        // pipe is median, V is inserted bucket      V       V        |              V|V
        StringToBuckets inputs = new StringToBuckets("a a a b c c c c d e e e e f f f g");
        assertEquals("buckets count", 7, inputs.buckets().size());
        BucketSampler<String> sampler = new BucketSampler<String>(2, inputs.inputsCount(), true);

        // insert one before everyone
        sampler.appendToResults(bucket("FIRST", 17, 100, 1000));

        // insert one right before a median
        sampler.add(inputs.popBucket()); // Bucket a
        sampler.add(inputs.popBucket()); // Bucket b
        sampler.appendToResults(bucket("SECOND", 23, 200, 2000));

        // insert one right after a median
        sampler.add(inputs.popBucket()); // Bucket c
        sampler.add(inputs.popBucket()); // Bucket d
        sampler.add(inputs.popBucket()); // Bucket e
        sampler.add(inputs.popBucket()); // Bucket f
        sampler.appendToResults(bucket("THIRD", 37, 300, 3000));

        // insert one at the end
        sampler.add(inputs.popBucket()); // Bucket 6
        sampler.appendToResults(bucket("FOURTH", 41, 400, 4000));
        
        assertEquals("emptied buckets() list", Collections.emptyList(), inputs.buckets());
        assertEquals("equality std dev", 1.39728d, sampler.getEqualsStdDev(), 0.00001d);
        assertEquals("equality mean", 2.42857d, sampler.getEqualsMean(), 0.00001d);
        List<Bucket<String>> expected = bucketsList(
                bucket("FIRST", 17, 100, 1000),
                bucket("SECOND", 23, 204, 2002),
                bucket("d", 1, 4, 1),
                bucket("THIRD", 37, 307, 3002),
                bucket("g", 1, 0, 0),
                bucket("FOURTH", 41, 400, 4000)
        );
        AssertUtils.assertCollectionEquals("compiled buckets", expected, sampler.buckets());
    }

    @Test
    public void testEqualityMean() {
        BucketSampler<String> sampler = runSampler(2, "a a a    b b   c c c c   d d d   e  f f f f f");
        assertEquals("mean equality", 3.0d, sampler.getEqualsMean(), 0.0);
    }

    @Test
    public void testEqualityStdDev() {
        BucketSampler<String> sampler = runSampler(2, "a a a    b b   c c c c   d d d   e  f f f f f ");
        assertEquals("equality std dev", 1.41421d, sampler.getEqualsStdDev(), 0.00001d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void maxIsZero() {
        new BucketSampler<String>(0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void maxIsNegative() {
        new BucketSampler<String>(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void expectedInputsIsNegative() {
        new BucketSampler<String>(16, -1);
    }
    
    @Test(expected = IllegalStateException.class)
    public void tooManyInputs() {
        BucketSampler<String> sampler = new BucketSampler<String>(16, 0);
        Bucket<String> bucket = new Bucket<String>();
        bucket.init("alpha", 1);
        sampler.add(bucket);
    }

    @Test(expected = IllegalStateException.class)
    public void tooFewInputs() {
        BucketSampler<String> sampler = new BucketSampler<String>(16, 1);
        sampler.buckets();
    }
    
    @Test(expected = IllegalStateException.class)
    public void stdDevRequestedButNotCalculated() {
        BucketSampler<String> sampler = runSampler(2, "a b c", false);
        sampler.getEqualsStdDev();
    }

    private BucketSampler<String> runSampler(int maxBuckets, String inputString) {
        return runSampler(maxBuckets, inputString, true);
    }

    private BucketSampler<String> runSampler(int maxBuckets, String inputString, boolean calculateStdDev) {
        StringToBuckets inputs = new StringToBuckets(inputString);
        BucketSampler<String> sampler = new BucketSampler<String>(maxBuckets, inputs.inputsCount(), calculateStdDev);
        for (Bucket<String> bucket : inputs.buckets())
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

    private class StringToBuckets {
        public StringToBuckets(String inputString) {
            String[] splits = inputString.length() == 0 ? new String[0] : inputString.split("\\s+");
            buckets = new ArrayList<Bucket<String>>();
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
            inputsCount = splits.length;
        }

        public int inputsCount() {
            return inputsCount;
        }

        public List<Bucket<String>> buckets() {
            return buckets;
        }

        public Bucket<String> popBucket() {
            return buckets.remove(0);
        }

        private int inputsCount;
        private List<Bucket<String>> buckets;
    }
}
