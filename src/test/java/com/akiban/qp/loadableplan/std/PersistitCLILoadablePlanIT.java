
package com.akiban.qp.loadableplan.std;

import static org.junit.Assert.*;

import java.util.List;

import com.akiban.server.types3.mcompat.mtypes.MString;
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
            queryContext.setPValue(0, new PValue(MString.varcharFor("stat"), "stat"));
            queryContext.setPValue(1, new PValue(MString.varcharFor("count=3"), "count=3"));
            queryContext.setPValue(2, new PValue(MString.varcharFor("delay=2"), "delay=2"));
            queryContext.setPValue(3, new PValue(MString.varcharFor("-a"), "-a"));
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
