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

package com.akiban.sql.pg;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessedGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.service.session.Session;

import java.io.*;
import java.util.*;

public class PostgresHapiOutputter implements HapiOutputter {
    private PostgresMessenger messenger;
    private Session session;
    private PostgresHapiRequest request;
    private int nrows, maxrows;

    public PostgresHapiOutputter(PostgresMessenger messenger, Session session,
                                 PostgresHapiRequest request, int maxrows) {
        this.messenger = messenger;
        this.session = session;
        // No way to get from HapiProcessedGetRequest to original HapiGetRequest.
        this.request = request;
        this.maxrows = maxrows;
        this.nrows = 0;
    }

    /** Return the number of rows output. */
    public int getNRows() {
        return nrows;
    }

    public void output(HapiProcessedGetRequest processedRequest, boolean hKeyOrdered,
                       Iterable<RowData> rows, OutputStream outputStream) 
            throws IOException {
        try {
            List<Column> columns = request.getColumns();
            List<PostgresType> types = request.getTypes();
            int ncols = columns.size();
            int[] tableIds = new int[ncols];
            for (int i = 0; i < ncols; i++) {
                tableIds[i] = -1;
            }
            int ntables = 256;                  // TODO: From where?
            byte[][] outputData = new byte[ncols][];
            boolean[] processedTableIds = new boolean[ntables];
            int outputTableId = -1;
            for (RowData rowData : rows) {
                if (messenger.isCancel()) {
                    nrows = -1;
                }
                NewRow row = new LegacyRowWrapper(rowData).niceRow();
                int tableId = row.getTableId();
                if (!processedTableIds[tableId]) {
                    RowDef rowDef = row.getRowDef();
                    UserTable table = rowDef.userTable();
                    if (table == request.getDeepestTable())
                        outputTableId = tableId;
                    for (int i = 0; i < ncols; i++) {
                        Column column = columns.get(i);
                        if (column.getTable() == table) {
                            tableIds[i] = tableId;
                        }
                    }
                    processedTableIds[tableId] = true;
                }
                for (int i = 0; i < ncols; i++) {
                    if (tableIds[i] == tableId) {
                        Column column = columns.get(i);
                        Object value = row.get(column.getPosition());
                        PostgresType type = types.get(i);
                        outputData[i] = type.encodeValue(value,
                                                         messenger.getEncoding(),
                                                         request.isColumnBinary(i));
                    }
                }
                if (tableId == outputTableId) {
                    sendDataRow(outputData);
                    nrows++;
                    if ((maxrows > 0) && (nrows >= maxrows))
                        return;
                }
            }
        }
        catch (StandardException ex) {
            throw new IOException(ex);
        }
    }

    // Send the current row, whose columns may come from ancestors.
    protected void sendDataRow(byte[][] row) throws IOException {
        messenger.beginMessage(PostgresMessenger.DATA_ROW_TYPE);
        messenger.writeShort(row.length);
        for (byte[] col : row) {
            if (col == null) {
                messenger.writeInt(-1);
            }
            else {
                messenger.writeInt(col.length);
                messenger.write(col);
            }
        }
        messenger.sendMessage();
    }

}
