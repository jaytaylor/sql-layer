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

package com.akiban.server.store;

import com.akiban.server.IndexDef;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowDef;
import com.akiban.server.TableStatistics;
import com.akiban.server.service.session.Session;
import com.persistit.exception.PersistitException;

public interface IndexManager {

    public abstract void analyzeTable(final Session session, final RowDef rowDef)
            throws Exception;

    public abstract void analyzeTable(final Session session,
            final RowDef rowDef, final int sampleSize) throws Exception;

    public abstract void deleteIndexAnalysis(final Session session,
            final IndexDef indexDef) throws PersistitException;

    public abstract void analyzeIndex(final Session session,
            final IndexDef indexDef, final int sampleSize)
            throws InvalidOperationException, PersistitException;

    public abstract void populateTableStatistics(final Session session,
            final TableStatistics tableStatistics) throws Exception;

}