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

package com.akiban.server.service.memcache.outputter.jsonoutputter;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessedGetRequest;
import com.akiban.util.AkibanAppender;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.*;

public final class JsonOutputter implements HapiOutputter
{
    public static JsonOutputter instance()
    {
        return INSTANCE;
    }

    @Override
    public void output(HapiProcessedGetRequest request,
                       boolean hKeyOrdered,
                       Iterable<RowData> rows,
                       OutputStream outputStream)
        throws IOException
    {
        new Request(request, hKeyOrdered, rows, outputStream).run();
    }

    private static final JsonOutputter INSTANCE = new JsonOutputter();
    final static int ROOT_PARENT = -1;

    private static class Request
    {
        public Request(HapiProcessedGetRequest request,
                       boolean hKeyOrdered,
                       Iterable<RowData> rows,
                       OutputStream outputStream)
            throws IOException
        {
            this.ais = request.akibanInformationSchema();
            queryRoot = rootTable(request);
            genealogist = new RowDataGenealogist(queryRoot, projectedTables(request));
            this.input = new UnOrphaningIterator<RowData>(new AncestorDiscoveryIterator(predicateTable(request),
                                                          hKeyOrdered,
                                                          rows.iterator()),
                                                   genealogist);
            this.output = new PrintWriter(outputStream);
            this.appender = AkibanAppender.of(outputStream, this.output);
        }

        public void run() throws IOException
        {
            advanceInput();
            output.write('{');
            int queryRootParentId =
                queryRoot.getParentJoin() == null ? ROOT_PARENT : queryRoot.getParentJoin().getParent().getTableId();
            generateChildOutput(queryRoot.getDepth(), queryRootParentId, true);
            output.write('}');
            output.flush();
        }

        private UserTable rootTable(HapiProcessedGetRequest request)
        {
            return ais.getUserTable(request.getSchema(), request.getTable());
        }

        private UserTable predicateTable(HapiProcessedGetRequest request)
        {
            return ais.getUserTable(request.getUsingTable());
        }

        private Set<UserTable> projectedTables(HapiProcessedGetRequest request)
        {
            Set<UserTable> projectedTables = new HashSet<UserTable>();
            String schemaName = request.getSchema();
            // Find the tables of interest
            for (String tableName : request.getProjectedTables()) {
                UserTable table = ais.getUserTable(schemaName, tableName);
                assert table != null : String.format("%s.%s", schemaName, tableName);
                projectedTables.add(table);
            }
            return projectedTables;
        }

        private void generateChildOutput(int depth, int parentTableId, boolean firstSibling) throws IOException
        {
            // Each pass through this loop will encounter a different child type. The rows of each type
            // are consecutive, and are handled by generateTableOutput. The missingChildren set tracks
            // observed types. Anything left at the end is a child of type parentTableId that had no rows
            // in the query result. These have to be rendered according to the spec.
            Set<Integer> missingChildren = new HashSet<Integer>(genealogist.expectedChildren(parentTableId));
            int previousRowTableId = -1;
            while (row != null && rowDepth == depth) {
                if (firstSibling) {
                    firstSibling = false;
                } else {
                    output.write(',');
                }
                Integer rowTableId = rowTable.getTableId();
                assert rowTableId != previousRowTableId : rowTable;
                missingChildren.remove(rowTableId);
                output.write("\"@");
                output.write(rowTable.getName().getTableName());
                output.write("\":[");
                generateTableOutput(rowTable);
                output.write(']');
                previousRowTableId = rowTableId;
            }
            assert rowDepth < depth : rowTable;
            // For each missing child: Generate output similar to above. But there were no rows,
            // so we'll see output of this form: "@foo":[]
            for (Integer tableId : missingChildren) {
                if (firstSibling) {
                    firstSibling = false;
                } else {
                    output.write(',');
                }
                UserTable table = ais.getUserTable(tableId);
                output.write("\"@");
                output.write(table.getName().getTableName());
                output.write("\":[]");
            }
        }
        
        private void generateTableOutput(UserTable table) throws IOException
        {
            // Generate output for consecutive rows of the same table
            int tableDepth = table.getDepth();
            boolean firstSibling = true;
            while (row != null && rowTable == table) {
                if (firstSibling) {
                    firstSibling = false;
                } else {
                    output.write(',');
                }
                // Generate output for row
                output.write('{');
                int rowTableId = rowTable.getTableId(); // Save this before going to the next row
                row.toJSONString(rowTable.rowDef(), appender);
                advanceInput();
                // We're now at a new row. If the new row is a child of the previous one, then
                // generateChildOutput then takes care of the children, (including children present
                // in the schema but not present in the data). If the next row is not a child of the previous
                // row, then generateChildOutput is still necessary to handle the missing children.
                assert rowDepth <= tableDepth + 1 : row;
                generateChildOutput(tableDepth + 1, rowTableId, false);
                output.write('}');
            }
        }
        
        private void advanceInput()
        {
            if (input.hasNext()) {
                row = input.next();
                rowTable = ais.getUserTable(row.getRowDefId());
                rowDepth = rowTable.getDepth();
            } else {
                row = null;
                rowTable = null;
                rowDepth = -1;
            }
        }

        private AkibanInformationSchema ais;
        private UserTable queryRoot;
        private RowDataGenealogist genealogist;
        private Iterator<RowData> input;
        private PrintWriter output;
        private AkibanAppender appender;
        private RowData row;
        private UserTable rowTable;
        private int rowDepth;
    }
}
