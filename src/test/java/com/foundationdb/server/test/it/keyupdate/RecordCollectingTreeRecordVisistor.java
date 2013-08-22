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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.store.TreeRecordVisitor;

import java.util.ArrayList;
import java.util.List;

class RecordCollectingTreeRecordVisistor extends TreeRecordVisitor
{
    @Override
    public void visit(Object[] key, NewRow row)
    {
        records.add(new TreeRecord(key, row));
    }

    public List<TreeRecord> records()
    {
        return records;
    }

    private final List<TreeRecord> records = new ArrayList<>();
}
