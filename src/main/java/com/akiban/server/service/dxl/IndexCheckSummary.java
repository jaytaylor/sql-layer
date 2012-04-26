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
        List<IndexCheckResult> results = new ArrayList<IndexCheckResult>();
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
        this.okResults = new ArrayList<IndexCheckResult>();
        this.fixedResults = new ArrayList<IndexCheckResult>();
        this.brokenResults = new ArrayList<IndexCheckResult>();
        this.otherResults = new ArrayList<IndexCheckResult>();
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
