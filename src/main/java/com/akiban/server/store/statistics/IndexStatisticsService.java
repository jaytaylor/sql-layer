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
