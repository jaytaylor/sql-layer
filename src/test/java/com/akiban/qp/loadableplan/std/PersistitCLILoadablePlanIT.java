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

package com.akiban.qp.loadableplan.std;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValue;

public class PersistitCLILoadablePlanIT extends ITBase {

    @Test
    public void invokePersistitOperation() throws Exception {
        PersistitCLILoadablePlan loadablePlan = new PersistitCLILoadablePlan();
        DirectObjectPlan plan = loadablePlan.plan();

        Schema schema = new Schema(ais());
        PersistitAdapter adapter = persistitAdapter(schema);
        QueryContext queryContext = queryContext(adapter);

        DirectObjectCursor cursor = plan.cursor(queryContext);
        if (Types3Switch.ON) {
            queryContext.setPValue(0, new PValue("stat"));
            queryContext.setPValue(1, new PValue("count=3"));
            queryContext.setPValue(2, new PValue("delay=2"));
            queryContext.setPValue(3, new PValue("-a"));
        }
        else {
            queryContext.setValue(0, new FromObjectValueSource().setReflectively("stat"));
            queryContext.setValue(1, new FromObjectValueSource().setReflectively("count=3"));
            queryContext.setValue(2, new FromObjectValueSource().setReflectively("delay=2"));
            queryContext.setValue(3, new FromObjectValueSource().setReflectively("-a"));
        }
        
        int populatedResults = 0;
        int emptyResults = 0;
        
        cursor.open();
        while(true) {
            List<? extends Object> columns = cursor.next();
            if (columns == null) {
                break;
            }
            if (columns.isEmpty()) {
                emptyResults++;
            } else {
                assertEquals(1, columns.size());
                assertTrue(columns.get(0) instanceof String);
                populatedResults++;
            }
        }
        cursor.close();
        assertEquals(3, populatedResults);
        assertTrue(emptyResults > 0 && emptyResults < 60);
    }
}
