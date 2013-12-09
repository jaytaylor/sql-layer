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

package com.foundationdb.server.service.listener;

import com.foundationdb.ais.model.Table;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.session.Session;
import com.persistit.Key;

public interface RowListener
{
    /** Called <i>after</i> group row and table indexes are written. */
    void onInsertPost(Session session, Table table, Key hKey, RowData row);

    /** Called <i>before</i> group row or table indexes are modified. */
    void onUpdatePre(Session session, Table table, Key hKey, RowData oldRow, RowData newRow);

    /** Called <i>after</i> group row and table indexes are modified. */
    void onUpdatePost(Session session, Table table, Key hKey, RowData oldRow, RowData newRow);

    /** Called <i>before</i> group row or table indexes are cleared. */
    void onDeletePre(Session session, Table table, Key hKey, RowData row);
}
