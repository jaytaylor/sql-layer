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

import com.akiban.server.RowData;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessedGetRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

public class RequestEchoOutputter implements HapiOutputter {

    private static final RequestEchoOutputter instance = new RequestEchoOutputter();

    public static RequestEchoOutputter instance() {
        return instance;
    }

    private RequestEchoOutputter()
    {}

    @Override
    public void output(HapiProcessedGetRequest request, List<RowData> rows,
                       OutputStream outputStream) throws IOException
    {
        PrintWriter writer = new PrintWriter(outputStream);
        writer.printf("Echoing request %s:\n", request);
        writer.printf("schema:       %s\n", request.getSchema());
        writer.printf("select table: %s\n", request.getTable());
        writer.printf("using  table: %s\n", request.getUsingTable());
        List<HapiGetRequest.Predicate> predicates = request.getPredicates();
        writer.printf("%d predicate%s\n", predicates.size(), predicates.size()==1 ? "" : "s");
        int predicateCount = 1;
        for (HapiGetRequest.Predicate predicate : predicates) {
            writer.printf("%d:\n", predicateCount++);
            writer.printf("  table:  %s\n", predicate.getTableName());
            writer.printf("  column: %s\n", predicate.getColumnName());
            writer.printf("  op:     %s\n", predicate.getOp());
            writer.printf("  value:  %s\n", predicate.getValue());
        }
        writer.printf("\nResult had %d row%s.", rows.size(), rows.size()==1 ? "" : "s");
        writer.flush();
    }
}
