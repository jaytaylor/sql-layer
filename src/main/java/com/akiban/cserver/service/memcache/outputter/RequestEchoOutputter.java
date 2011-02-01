package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.HapiProcessor;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

public class RequestEchoOutputter implements HapiProcessor.Outputter {

    private static final RequestEchoOutputter instance = new RequestEchoOutputter();

    public static RequestEchoOutputter instance() {
        return instance;
    }

    private RequestEchoOutputter()
    {}

    @Override
    public void output(HapiGetRequest request, RowDefCache rowDefCache, List<RowData> rows,
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
