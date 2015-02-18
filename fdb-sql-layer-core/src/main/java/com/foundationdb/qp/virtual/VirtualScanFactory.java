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

package com.foundationdb.qp.virtual;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.session.Session;

import static com.foundationdb.qp.virtual.VirtualGroupCursor.GroupScan;

public interface VirtualScanFactory
{
    public TableName getName();
    
    public GroupScan getGroupScan(VirtualAdapter adapter, Group group);

    public long rowCount(Session session);
}
