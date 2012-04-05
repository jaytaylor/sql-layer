/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.service.memcache.outputter;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiPredicate;
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
    public void output(HapiProcessedGetRequest request,
                       boolean hKeyOrdered,
                       Iterable<RowData> rows,
                       OutputStream outputStream) throws IOException
    {
        PrintWriter writer = new PrintWriter(outputStream);
        writer.printf("Echoing request %s:\n", request);
        writer.printf("schema:       %s\n", request.getSchema());
        writer.printf("select table: %s\n", request.getTable());
        writer.printf("using  table: %s\n", request.getUsingTable());
        List<HapiPredicate> predicates = request.getPredicates();
        writer.printf("%d predicate%s\n", predicates.size(), predicates.size()==1 ? "" : "s");
        int predicateCount = 1;
        for (HapiPredicate predicate : predicates) {
            writer.printf("%d:\n", predicateCount++);
            writer.printf("  table:  %s\n", predicate.getTableName());
            writer.printf("  column: %s\n", predicate.getColumnName());
            writer.printf("  op:     %s\n", predicate.getOp());
            writer.printf("  value:  %s\n", predicate.getValue());
        }
        writer.flush();
    }
}
