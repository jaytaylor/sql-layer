/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.store.statistics;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.ais.model.Index;

import java.io.Writer;
import java.util.Collection;
import java.io.File;
import java.io.IOException;

public interface IndexStatisticsService
{
    public final static TableName INDEX_STATISTICS_TABLE_NAME = new TableName(TableName.INFORMATION_SCHEMA, "index_statistics");
    public final static TableName INDEX_STATISTICS_ENTRY_TABLE_NAME = new TableName(INDEX_STATISTICS_TABLE_NAME.getSchemaName(), "index_statistics_entry");

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

    /** Delete stored statistics for a schema */
    public void deleteIndexStatistics(Session session,
                                      String schema) throws IOException;

    /** Clear the in-memory cache. */
    public void clearCache();

    /** How many buckets to compute per index */
    public int bucketCount();

    /** Note missing statistics: warn user, initiate background analyze. */
    public void missingStats(Session session, Index index, Column column);

    /** Check for out of date stats, based on table being much larger. */
    public void checkRowCountChanged(Session session, Table table,
                                     IndexStatistics stats, long rowCount);
}
