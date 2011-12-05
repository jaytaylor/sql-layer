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

import static com.akiban.server.store.statistics.IndexStatistics.*;

import com.akiban.ais.model.Index;
import com.akiban.server.store.IndexVisitor;

import com.persistit.Key;
import com.persistit.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PersistitIndexStatisticsVisitor extends IndexVisitor
{
    private static final Logger logger = LoggerFactory.getLogger(PersistitIndexStatisticsVisitor.class);
    
    private Index index;

    public PersistitIndexStatisticsVisitor(Index index) {
        this.index = index;
    }

    protected void visit(Key key, Value value) {
        logger.debug("Key = " + key + ", Value = " + value);
    }

    public IndexStatistics getIndexStatistics() {
        return null;
    }

}
