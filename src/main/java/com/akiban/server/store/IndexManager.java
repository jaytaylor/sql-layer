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

import com.akiban.ais.model.Index;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.TableStatistics;
import com.akiban.server.service.session.Session;
import com.persistit.exception.PersistitException;
import java.util.Collection;

public interface IndexManager {

    public void analyzeTable(Session session, RowDef rowDef);

    public void analyzeTable(Session session, RowDef rowDef, int sampleSize);

    public void deleteIndexAnalysis(Session session, Index index) throws PersistitException;

    public void analyzeIndex(Session session, Index index, int sampleSize) throws PersistitException;

    public void populateTableStatistics(Session session, TableStatistics tableStatistics) throws PersistitException;
}
