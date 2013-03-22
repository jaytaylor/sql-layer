
package com.akiban.server.service.dxl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public final class IndexCheckSummary {

    public List<IndexCheckResult> getOkResults() {
        return okResults;
    }

    public List<IndexCheckResult> getFixedResults() {
        return fixedResults;
    }

    public List<IndexCheckResult> getBrokenResults() {
        return brokenResults;
    }

    public List<IndexCheckResult> getOtherResults() {
        return otherResults;
    }

    public List<IndexCheckResult> getAllResults() {
        List<IndexCheckResult> results = new ArrayList<>();
        results.addAll(okResults);
        results.addAll(fixedResults);
        results.addAll(brokenResults);
        results.addAll(otherResults);
        return results;
    }

    public long getTimeNanoseconds() {
        return timeNs;
    }

    public String getTimeSeconds() {
        double seconds = ((double)timeNs) / 1000000000.0;
        return String.format("%2f", seconds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexCheckSummary that = (IndexCheckSummary) o;

        return brokenResults.equals(that.brokenResults)
                && fixedResults.equals(that.fixedResults)
                && okResults.equals(that.okResults)
                && otherResults.equals(that.otherResults);

    }

    @Override
    public int hashCode() {
        int result = okResults.hashCode();
        result = 31 * result + fixedResults.hashCode();
        result = 31 * result + brokenResults.hashCode();
        result = 31 * result + otherResults.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("ok: %s, fixed: %s, broken: %s, other:%s (%s sec)",
                okResults,  fixedResults, brokenResults, otherResults, getTimeSeconds());
    }

    public IndexCheckSummary(List<IndexCheckResult> results, long timeNs) {
        this.timeNs = timeNs;
        this.okResults = new ArrayList<>();
        this.fixedResults = new ArrayList<>();
        this.brokenResults = new ArrayList<>();
        this.otherResults = new ArrayList<>();
        for (IndexCheckResult result : results) {
            List<IndexCheckResult> which;
            switch (result.getBottomLine()) {
            case OK:    which = okResults;    break;
            case FIXED: which = fixedResults; break;
            case BROKEN:which = brokenResults;break;
            default:    which = otherResults; break;
            }
            which.add(result);
        }
        Collections.sort(okResults, ICR_COMPARATOR);
        Collections.sort(fixedResults, ICR_COMPARATOR);
        Collections.sort(brokenResults, ICR_COMPARATOR);
        Collections.sort(otherResults, ICR_COMPARATOR);
    }

    private final long timeNs;
    private final List<IndexCheckResult> okResults;
    private final List<IndexCheckResult> fixedResults;
    private final List<IndexCheckResult> brokenResults;
    private final List<IndexCheckResult> otherResults;

    private static final Comparator<IndexCheckResult> ICR_COMPARATOR = new Comparator<IndexCheckResult>() {
        @Override
        public int compare(IndexCheckResult o1, IndexCheckResult o2) {
            return o1.getIndexName().compareTo(o2.getIndexName());
        }
    };
}
