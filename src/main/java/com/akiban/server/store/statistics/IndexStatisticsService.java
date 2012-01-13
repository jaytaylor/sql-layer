/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.store.statistics;

import com.akiban.server.service.session.Session;
import com.akiban.ais.model.Index;
import com.persistit.exception.PersistitInterruptedException;

import java.util.Collection;
import java.io.File;
import java.io.IOException;

public interface IndexStatisticsService
{
    /** Get <em>approximate</em> count of number of entries in the given index. */
    public long countEntries(Session session, Index index);
    
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
                                    String schema, File file) throws IOException;
}
