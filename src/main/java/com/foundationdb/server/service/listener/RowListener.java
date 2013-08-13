/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.service.listener;

import com.akiban.ais.model.UserTable;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.service.session.Session;
import com.persistit.Key;

public interface RowListener
{
    void onWrite(Session session, UserTable table, Key hKey, RowData row);
    void onUpdate(Session session, UserTable table, Key hKey, RowData oldRow, RowData newRow);
    void onDelete(Session session, UserTable table, Key hKey, RowData row);
}
