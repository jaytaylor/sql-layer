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

package com.foundationdb.server.api;

import java.util.List;
import java.util.Set;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.service.session.Session;

public interface DMLFunctions {

    /**
     * Wraps a RowData in a NewRow. This conversion requires a RowDef, which the caller may not have, but which
     * implementers of this interface should.
     * @param rowData the row to wrap
     * @return a NewRow representation of the RowData
     */
    NewRow wrapRowData(Session session, RowData rowData);

    /**
     * Converts a RowData to a NewRow. This conversion requires a RowDef, which the caller may not have, but which
     * implementers of this interface should.
     * @param rowData the row to convert
     * @return a NewRow representation of the RowData
     */
    NewRow convertRowData(Session session, RowData rowData);

    /**
     * Converts several RowData objects at once. This is not just a convenience; it lets implementations of this
     * class cache RowDefs they need, which could save time.
     * @param rowDatas the rows to convert
     * @return a List of NewRows, each of which is a converted RowData
     */
    List<NewRow> convertRowDatas(Session session, List<RowData> rowDatas);

    /**
     * Truncates the given table.
     *
     * @param tableId the table to truncate
     */
    void truncateTable(Session session, int tableId);

    /**
     * Truncates the given table, possibly cascading the truncate to child tables.
     *
     * @param tableId the table to truncate
     * @param descendants <code>true</code> to delete descendants too
     */
    void truncateTable(Session session, int tableId, boolean descendants);
}
