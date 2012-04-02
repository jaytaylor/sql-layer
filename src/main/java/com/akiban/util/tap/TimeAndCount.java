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

package com.akiban.util.tap;

/**
 * A Tap subclass that counts and times the intervals between calls to
 * {@link #in()} and {@link #out()}.
 */
class TimeAndCount extends Tap
{
    // Object interface

    public String toString()
    {
        return String.format("%s inCount=%,d outCount=%,d time=%,dms",
                             name, inCount, outCount, cumulativeNanos / 1000000);
    }

    // Tap interface

    public void in()
    {
        justEnabled = false;
        checkNesting();
        inCount++;
        inNanos = System.nanoTime();
    }

    public void out()
    {
        if (justEnabled) {
            justEnabled = false;
        } else {
            outCount++;
            boolean nestingOK = checkNesting();
            justEnabled = false;
            long now = System.nanoTime();
            endNanos = now;
            if (nestingOK) {
                lastDuration = now - inNanos;
                cumulativeNanos += lastDuration;
            }
            // else: Usage of this tap is non-nested. checkNesting() reported on the problem. But skip
            // maintenance and use of lastDuration to try and keep reported values approximately right.
        }
    }

    public long getDuration()
    {
        return lastDuration;
    }

    public void reset()
    {
        inCount = 0;
        outCount = 0;
        cumulativeNanos = 0;
        justEnabled = true;
    }

    public void appendReport(StringBuilder buffer)
    {
        buffer.append(String.format("%20s inCount=%,10d outCount=%,10d time=%,12dms",
                                name, inCount, outCount, cumulativeNanos / 1000000));
        if (outCount > 0) {
            buffer.append(String.format("  per=%,12dns  interval=%,12dns",
                                    cumulativeNanos / outCount, (endNanos - startNanos) / outCount));
        }
    }

    public TapReport[] getReports()
    {
        return new TapReport[]{new TapReport(getName(), inCount, outCount, cumulativeNanos)};
    }

    // TimeAndCount interface

    public TimeAndCount(String name)
    {
        super(name);
    }

    // Object state

    private volatile long cumulativeNanos;
    private volatile long inNanos;
    private volatile long startNanos = System.nanoTime();
    private volatile long endNanos;
    private volatile long lastDuration = Long.MIN_VALUE;
    private volatile boolean justEnabled = false;
}
