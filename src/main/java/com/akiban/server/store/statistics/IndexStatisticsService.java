
package com.akiban.server.store.statistics;

import com.akiban.ais.model.TableName;
import com.akiban.server.service.session.Session;
import com.akiban.ais.model.Index;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

import java.io.Writer;
import java.util.Collection;
import java.io.File;
import java.io.IOException;

public interface IndexStatisticsService
{
    public final static TableName INDEX_STATISTICS_TABLE_NAME = new TableName(TableName.INFORMATION_SCHEMA, "index_statistics");
    public final static TableName INDEX_STATISTICS_ENTRY_TABLE_NAME = new TableName(INDEX_STATISTICS_TABLE_NAME.getSchemaName(), "index_statistics_entry");

    /** Get current count of number of entries in the given index. */
    public long countEntries(Session session, Index index) throws PersistitInterruptedException;
    
    /** Get <em>approximate</em> count of number of entries in the given index. */
    public long countEntriesApproximate(Session session, Index index);

    public long countEntriesManually(Session session, Index index) throws PersistitException;
    
    /** Get available statistics for the given index. */
    public IndexStatistics getIndexStatistics(Session session, Index index);

    /** Update statistics for the given indexes. */
    public void updateIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes);

    /** Delete stored statistics for the given indexes. */
    public void deleteIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes);

    /** Load statistics from a YAML file. */
    public void loadIndexStatistics(Session session, 
                                    String schema, File file) throws IOException;

    /** Dump statistics to a YAML file. */
    public void dumpIndexStatistics(Session session, 
                                    String schema, Writer file) throws IOException;

    /** Clear the in-memory cache. */
    public void clearCache();
}
