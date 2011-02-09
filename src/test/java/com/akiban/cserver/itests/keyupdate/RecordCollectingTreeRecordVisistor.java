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

package com.akiban.cserver.itests.keyupdate;

import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.store.TreeRecordVisitor;

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

    private final List<TreeRecord> records = new ArrayList<TreeRecord>();
}