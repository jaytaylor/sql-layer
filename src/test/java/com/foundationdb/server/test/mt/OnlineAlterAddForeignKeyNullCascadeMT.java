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

package com.foundationdb.server.test.mt;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.dxl.OnlineDDLMonitor;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.MonitoredThread;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadMonitor;
import com.foundationdb.server.test.mt.util.TimeMarkerComparison;
import org.junit.Test;

import java.util.List;

/** Interleaved DML during an ALTER ADD FOREIGN KEY ON UPDATE SET NULL ON DELETE CASCADE. */
public class OnlineAlterAddForeignKeyNullCascadeMT extends OnlineAlterAddForeignKeyCascadeNullMT
{
    protected static final String ALTER_ADD_FK = "ALTER TABLE "+CHILD_TABLE+" ADD CONSTRAINT fk1 FOREIGN KEY(pid) "+
                                                 "REFERENCES "+PARENT_TABLE+"(pid) ON UPDATE SET NULL ON DELETE CASCADE";

    @Override
    protected String getDDL() {
        return ALTER_ADD_FK;
    }

    // Note: Not actually violations with CASCADE or SET NULL

    @Override
    @Test
    public void updateViolationPostMetaToPreFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        Row newRow = testRow(parentRowType, 3);
        dmlPostMetaToPreFinal(updateCreator(pID, oldRow, newRow),
                              replace(parentGroupRows, 1, newRow),
                              replace(childGroupRows, 1, testRow(childRowType, 20, null)));
    }

    @Override
    @Test
    public void deleteViolationPostMetaToPreFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        dmlPostMetaToPreFinal(deleteCreator(pID, oldRow),
                              remove(parentGroupRows, 1),
                              remove(childGroupRows, 1));
    }
}
