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

import com.akiban.util.ArgumentValidation;
import com.akiban.util.Flywheel;
import com.akiban.util.Recycler;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

/**
 * Combines the SplitHandler (T visitor) with Buckets (T aggregator).
 * @param <T> the thing to be sampled
 */
public class Sampler<T extends Comparable<? super T>> extends SplitHandler<T> {
    @Override
    protected void handle(int segmentIndex, T input, int count) {
        BucketSampler<T> bucketSampler = bucketSamplerList.get(segmentIndex);
        Bucket<T> bucket = bucketsFlywheel.get();
        bucket.init(input, count);
        if (!bucketSampler.add(bucket))
            bucketsFlywheel.recycle(bucket);
    }

    @Override
    public void finish() {
        super.finish();
        finished = true;
    }

    public List<List<Bucket<T>>> toBuckets() {
        if (!finished) {
            throw new IllegalStateException("never called finish() after visiting");
        }
        List<PopularitySplit<T>> popularitySplits = splitStreamsByPopularity();
        return mergePopularitySplitStreams(popularitySplits);
    }

    private List<PopularitySplit<T>> splitStreamsByPopularity() {
        List<PopularitySplit<T>> results = new ArrayList<PopularitySplit<T>>(segments);
        for (BucketSampler<T> sampler : bucketSamplerList) {
            PopularitySplit<T> popularitySplit = splitByPopularity(sampler);
            results.add(popularitySplit);
        }
        return results;
    }

    private PopularitySplit<T> splitByPopularity(BucketSampler<T> sampler) {
        List<Bucket<T>> samples = sampler.buckets();
        Deque<Bucket<T>> popular = new ArrayDeque<Bucket<T>>(samples.size());
        int popularsCount = 0;
        int regularsCount = 0;

        // a bucket is "exceptionally popular" if its popularity (equals-count) is more than one standard dev
        // above average. Also keep the first (min key) bucket.
        long popularityCutoff = Math.round(sampler.getEqualsMean() + sampler.getEqualsStdDev());
        for (Iterator<Bucket<T>> iter = samples.iterator(); iter.hasNext(); ) {
            Bucket<T> sample = iter.next();
            long sampleCount = sample.getEqualsCount() + sample.getLessThanCount();
            if (sample.isMinKeyBucket() || sample.getEqualsCount() >= popularityCutoff) {
                iter.remove();
                popular.add(sample);
                popularsCount += sampleCount;
            }
            else {
                regularsCount += sampleCount;
            }
        }
        return new PopularitySplit<T>(regularsCount, samples, popularsCount, popular);
    }

    private List<List<Bucket<T>>> mergePopularitySplitStreams(List<PopularitySplit<T>> popularitySplits) {
        List<List<Bucket<T>>> results = new ArrayList<List<Bucket<T>>>(popularitySplits.size());
        for (PopularitySplit<T> split : popularitySplits) {
            List<Bucket<T>> merged = mergePopularitySplit(split);
            results.add(merged);
        }
        return results;
    }


    private List<Bucket<T>> mergePopularitySplit(PopularitySplit<T> split) {
        // if populars.size() >= maxSize,
        //     we sample the populars, merging the unpopular buckets into the sampled populars as we go
        // if populars.size() < maxSize,
        //     we sample the unpopulars, appending the popular buckets into the sampling as we go
        return split.popularBuckets.size() >= maxSize
                ? mergeUnpopularsIntoPopulars(split)
                : mergePopularsIntoUnpopulars(split);
    }

    private List<Bucket<T>> mergePopularsIntoUnpopulars(PopularitySplit<T> split) {
        Deque<Bucket<T>> populars = split.popularBuckets;
        assert populars.size() < maxSize : "failed populars.size[" + populars.size() + "] < maxSize[" + maxSize + "]";
        int unpopularsNeeded = maxSize - populars.size();
        BucketSampler<T> sampler = new BucketSampler<T>(unpopularsNeeded, split.regularsCount, false);
        for (Bucket<T> regularBucket : split.regularBuckets) {
            assert !regularBucket.isMinKeyBucket(); // Min-key buckets were deemed popular
            T regularValue = regularBucket.value();
            while (!populars.isEmpty()) {
                Bucket<T> popularBucket = populars.getFirst();
                // The min-key bucket, by definition, comes before any bucket, including regularBucket.
                // But there is an explicit test for the min-key bucket to make it clear that it must be included.
                if (popularBucket.isMinKeyBucket() || popularBucket.value().compareTo(regularValue) < 0)
                    sampler.appendToResults(populars.removeFirst()); // and the loop will try again
                else
                    break;
            }
            sampler.add(regularBucket);
        }
        for (Bucket<T> popularBucket : populars)
            sampler.appendToResults(popularBucket);
        return sampler.buckets();
    }

    private List<Bucket<T>> mergeUnpopularsIntoPopulars(PopularitySplit<T> split) {
        Deque<Bucket<T>> populars = split.popularBuckets;
        assert populars.size() >= maxSize : "failed  populars.size[" + populars.size() + "] >= maxSize[" + maxSize + "]";
        PeekingIterator<Bucket<T>> unpopulars = Iterators.peekingIterator(split.regularBuckets.iterator());
        List<Bucket<T>> results = new ArrayList<Bucket<T>>(populars.size());
        BucketSampler<T> sampler = new BucketSampler<T>(maxSize, split.popularsCount, false);
        for (Bucket<T> popular : populars) {
            if (sampler.add(popular)) {
                // merge in all the unpopulars less than this one
                while (unpopulars.hasNext() && unpopulars.peek().value().compareTo(popular.value()) <= 0) {
                    Bucket<T> mergeMe = unpopulars.next();
                    mergeUp(mergeMe, popular);
                }
                results.add(popular);
            }
        }
        // now, create one last value which merges in all of the remaining unpopulars
        Bucket<T> last = null;
        while(unpopulars.hasNext()) {
            Bucket<T> unpopular = unpopulars.next();
            if (last != null)
                mergeUp(last,  unpopular);
            last = unpopular;
        }
        if (last != null)
            results.add(last);
        
        return results;
    }

    private void mergeUp(Bucket<T> from, Bucket<T> into) {
        into.addLessThanDistincts(from.getLessThanDistinctsCount() + 1);
        into.addLessThans(from.getLessThanCount() + from.getEqualsCount());
    }

    public Sampler(Splitter<T> splitter, int maxSize, long expectedInputs, Recycler<? super T> recycler) {
        super(splitter);
        int segments = splitter.segments();
        ArgumentValidation.isGT("segments", segments, 0);
        bucketSamplerList =  new ArrayList<BucketSampler<T>>(segments);
        this.maxSize = maxSize;
        int oversampleSize = maxSize * OVERSAMPLE_FACTOR;
        for (int i=0; i < segments; ++i) {
            bucketSamplerList.add(new BucketSampler<T>(oversampleSize, expectedInputs));
        }
        this.segments = segments;
        this.bucketsFlywheel = new BucketFlywheel<T>(oversampleSize, segments, recycler);
    }

    private final List<BucketSampler<T>> bucketSamplerList;
    private final int segments;
    private final int maxSize;
    private boolean finished = false;
    private final Flywheel<Bucket<T>> bucketsFlywheel;

    public static final int OVERSAMPLE_FACTOR = 50;

    private static class PopularitySplit<T> {
        private PopularitySplit(int regularsCount, List<Bucket<T>> regularBuckets,
                                int popularsCount, Deque<Bucket<T>> popularBuckets
        ) {
            this.regularBuckets = regularBuckets;
            this.popularBuckets = popularBuckets;
            this.popularsCount = popularsCount;
            this.regularsCount = regularsCount;
        }

        private final List<Bucket<T>> regularBuckets;
        private final Deque<Bucket<T>> popularBuckets;
        private final int popularsCount;
        private final int regularsCount;
    }

    private static class BucketFlywheel<T> extends Flywheel<Bucket<T>> {
        @Override
        protected Bucket<T> createNew() {
            ++created;
            assert created <= createdLimit : created + " > " + createdLimit;
            return new Bucket<T>();
        }

        @Override
        public void recycle(Bucket<T> element) {
            super.recycle(element);
            valueRecycler.recycle(element.value());
        }

        private BucketFlywheel(int maxSize, int segments, Recycler<? super T> valueRecycler) {
            this.createdLimit = (maxSize+1) * segments;
            this.valueRecycler = valueRecycler;
        }

        private final Recycler<? super T> valueRecycler;
        private final int createdLimit;
        private int created;
    }
}
