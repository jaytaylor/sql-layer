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

package com.akiban.server.store.statistics;

public class HistogramEntryDescription
{

    protected String keyString;
    protected long equalCount;
    protected long lessCount;
    protected long distinctCount;

    public HistogramEntryDescription(String keyString, long equalCount, long lessCount, long distinctCount) {
        this.distinctCount = distinctCount;
        this.equalCount = equalCount;
        this.keyString = keyString;
        this.lessCount = lessCount;
    }

    /** A user-visible form of the key for this entry. */
    public String getKeyString() {
        return keyString;
    }

    /** The number of samples that were equal to the key value. */
    public long getEqualCount() {
        return equalCount;
    }

    /** The number of samples that were less than the key value
     * (and greater than the previous entry's key value, if any).
     */
    public long getLessCount() {
        return lessCount;
    }

    /** The number of distinct values in the less-than range. */
    public long getDistinctCount() {
        return distinctCount;
    }

    @Override
    final public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HistogramEntryDescription)) return false;

        HistogramEntryDescription that = (HistogramEntryDescription) o;

        return distinctCount == that.distinctCount
                && equalCount == that.equalCount
                && lessCount == that.lessCount
                && keyString.equals(that.keyString);

    }

    @Override
    final public int hashCode() {
        int result = keyString.hashCode();
        result = 31 * result + (int) (equalCount ^ (equalCount >>> 32));
        result = 31 * result + (int) (lessCount ^ (lessCount >>> 32));
        result = 31 * result + (int) (distinctCount ^ (distinctCount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" + getKeyString() +
                ": = " + getEqualCount() +
                ", < " + getLessCount() +
                ", distinct " + getDistinctCount() +
                "}";
    }
}
