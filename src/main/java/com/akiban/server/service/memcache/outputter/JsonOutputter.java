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

package com.akiban.server.service.memcache.outputter;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
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
    public void output(HapiProcessedGetRequest request, List<RowData> rows, OutputStream outputStream)
        throws IOException
    {
        new Request(request, rows, outputStream).run();
    }

    private static final JsonOutputter INSTANCE = new JsonOutputter();
    private final static int QUERY_ROOT_PARENT = -1;

    private static class Request
    {
        public Request(HapiProcessedGetRequest request, List<RowData> rows, OutputStream outputStream)
            throws IOException
        {
            this.request = request;
            this.ais = request.akibanInformationSchema();
            computeExpectedChildren(request);
            this.input = rows.iterator();
            this.output = new PrintWriter(outputStream);
            this.appender = AkibanAppender.of(outputStream, this.output);
        }

        public void run() throws IOException
        {
            advanceInput();
            output.write('{');
            UserTable queryRoot = queryRoot(request);
            generateChildOutput(queryRoot.getDepth(), QUERY_ROOT_PARENT, true);
            output.write('}');
            output.flush();
        }

        private void generateChildOutput(int depth, int parentTableId, boolean firstSibling) throws IOException
        {
            // Each pass through this loop will encounter a different child type. The rows of each type
            // are consecutive, and are handled by generateTableOutput. The missingChildren set tracks
            // observed types. Anything left at the end is a child of type parentTableId that had no rows
            // in the query result. These have to be rendered according to the spec.
            Set<Integer> missingChildren = new HashSet<Integer>(expectedChildren.get(parentTableId));
            int previousTableId = -1;
            while (row != null && rowDepth == depth) {
                if (firstSibling) {
                    firstSibling = false;
                } else {
                    output.write(',');
                }
                Integer tableId = rowTable.getTableId();
                assert tableId != previousTableId : rowTable;
                missingChildren.remove(tableId);
                output.write("\"@");
                output.write(rowTable.getName().getTableName());
                output.write("\":[");
                generateTableOutput(rowTable);
                output.write(']');
                previousTableId = tableId;
            }
            assert rowDepth < depth : rowTable;
            // For each missing child: Generate output similar to above, except that there were no rows.
            // So we'll see output of this form: "@foo":[]
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
            boolean firstSibling = true;
            while (row != null && rowTable == table) {
                if (firstSibling) {
                    firstSibling = false;
                } else {
                    output.write(',');
                }
                // Generate output for row
                output.write('{');
                int tableId = rowTable.getTableId();
                row.toJSONString((RowDef) rowTable.rowDef(), appender);
                advanceInput();
                // Go to the next row. generateChildOutput then takes care of the children, including children present
                // in the schema but not present in the data. If the next row is not actually a child, then
                // generateChildOutput is still necessary to handle the missing children.
                Integer tableDepth = table.getDepth();
                assert rowDepth <= tableDepth + 1;
                generateChildOutput(tableDepth + 1, tableId, false);
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

        private UserTable queryRoot(HapiProcessedGetRequest request)
        {
            return ais.getUserTable(request.getSchema(), request.getTable());
        }

        private void computeExpectedChildren(HapiProcessedGetRequest request)
        {
            expectedChildren = new HashMap<Integer, Set<Integer>>();
            // Find the tables of interest
            List<UserTable> tables = new ArrayList<UserTable>();
            String schemaName = request.getSchema();
            for (String tableName : request.getProjectedTables()) {
                UserTable table = ais.getUserTable(schemaName, tableName);
                assert table != null : String.format("%s.%s", schemaName, tableName);
                tables.add(table);
            }
            // Sort by depth so that expectedChildren can be computed in one pass
            Collections.sort(tables,
                             new Comparator<UserTable>()
                             {
                                 @Override
                                 public int compare(UserTable x, UserTable y)
                                 {
                                     return x.getDepth() - y.getDepth();
                                 }
                             });
            // For each table in tables, add table to expectedChildren of parent.
            for (UserTable table : tables) {
                Set<Integer> replaced = expectedChildren.put(table.getTableId(), new HashSet<Integer>());
                assert replaced == null : table;
                Join parentJoin = table.getParentJoin();
                if (parentJoin != null) {
                    UserTable parent = parentJoin.getParent();
                    Set<Integer> expectedChildrenOfParent = expectedChildren.get(parent.getTableId());
                    if (expectedChildrenOfParent != null) {
                        expectedChildrenOfParent.add(table.getTableId());
                    }
                }
            }
            // Set expected children of the entire query. Because of sorting, query root is at position 0.
            expectedChildren.put(QUERY_ROOT_PARENT, new HashSet<Integer>(Arrays.asList(tables.get(0).getTableId())));
        }

        private HapiProcessedGetRequest request;
        private AkibanInformationSchema ais;
        private Map<Integer, Set<Integer>> expectedChildren;
        private Iterator<RowData> input;
        private PrintWriter output;
        private AkibanAppender appender;
        private RowData row;
        private UserTable rowTable;
        private int rowDepth;
    }
}
