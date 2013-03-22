
package com.akiban.server.store.statistics;

import java.io.IOException;

public interface IndexStatisticsMXBean
{
    /** Write index statistics to a YAML file. */
    public String dumpIndexStatistics(String schema, String toFile) throws IOException;

    /** Return index statistics as a String */
    public String dumpIndexStatisticsToString(String schema) throws IOException;

    /** Read index statistics from a YAML file. */
    public void loadIndexStatistics(String schema, String fromFile) throws IOException;
}
