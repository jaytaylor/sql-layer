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
package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.GroupIndex;
import com.akiban.server.RowData;
import com.akiban.server.service.session.Session;
import com.persistit.exception.PersistitException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TestOperatorStore extends OperatorStore {

    // TestOperatorStore interface

    public <T extends Throwable>
    void testMaintainGroupIndexes(Session session, RowData rowData, GroupIndexHandler<T> handler)
            throws PersistitException, T
    {
        super.maintainGroupIndexes(session, rowData, handler);
    }

    // OperatorStore overrides


    @Override
    protected Collection<GroupIndex> optionallyOrderGroupIndexes(Collection<GroupIndex> groupIndexes) {
        List<GroupIndex> ordered = new ArrayList<GroupIndex>(groupIndexes);
        Collections.sort(ordered, GI_COMPARATOR);
        return ordered;
    }

    // nested classes
    public static interface GroupIndexHandler<T extends Throwable> extends OperatorStore.GroupIndexHandler<T> {
        // promoting visibility
    }

    // consts

    private static final Comparator<GroupIndex> GI_COMPARATOR = new Comparator<GroupIndex>() {
        @Override
        public int compare(GroupIndex o1, GroupIndex o2) {
            String o1Name = o1.getIndexName().getName();
            String o2Name = o2.getIndexName().getName();
            return o1Name.compareTo(o2Name);
        }
    };
}
