
package com.akiban.util.tap;

/**
 * A Tap subclass that counts calls to {@link #in()} and {@link #out()}.
 * Generally this is faster than {@link TimeAndCount} because the system
 * clock is not read.
 */
class Count extends Tap
{
    // Object interface

    public String toString()
    {
        return String.format("%s inCount=%,d outCount=%,d", name, inCount, outCount);
    }

    // Tap interface

    public void in()
    {
        justEnabled = false;
        checkNesting();
        inCount++;
    }

    public void out()
    {
        if (justEnabled) {
            justEnabled = false;
        } else {
            outCount++;
            checkNesting();
        }
    }

    public long getDuration()
    {
        return 0;
    }

    public void reset()
    {
        inCount = 0;
        outCount = 0;
        justEnabled = true;
    }

    public void appendReport(String label, StringBuilder buffer)
    {
        buffer.append(String.format("%s %20s inCount=%,10d outCount=%,10d", label, name, inCount, outCount));
    }

    public TapReport[] getReports()
    {
        return new TapReport[]{new TapReport(name, inCount, outCount, 0)};
    }

    // Count interface

    public Count(String name)
    {
        super(name);
    }

    // Object state

    private volatile boolean justEnabled = false;
}
