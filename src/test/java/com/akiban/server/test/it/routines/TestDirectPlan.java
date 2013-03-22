
package com.akiban.server.test.it.routines;

import com.akiban.qp.loadableplan.LoadableDirectObjectPlan;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.qp.operator.QueryContext;

import java.sql.Types;

import java.util.Arrays;
import java.util.List;

/** A loadable direct object plan.
 * <code><pre>
CALL sqlj.install_jar('target/akiban-server-1.4.3-SNAPSHOT-tests.jar', 'testjar', 0);
CREATE PROCEDURE test_direct(IN n BIGINT) LANGUAGE java PARAMETER STYLE akiban_loadable_plan EXTERNAL NAME 'testjar:com.akiban.server.test.it.routines.TestDirectPlan';
CALL test_direct(10);
 * </pre></code> 
 */
public class TestDirectPlan extends LoadableDirectObjectPlan
{
    @Override
    public DirectObjectPlan plan()
    {
        return new DirectObjectPlan() {
                @Override
                public DirectObjectCursor cursor(QueryContext context) {
                    return new TestDirectObjectCursor(context);
                }
            };
    }

    public static class TestDirectObjectCursor extends DirectObjectCursor {
        private QueryContext context;
        private long i, n;

        public TestDirectObjectCursor(QueryContext context) {
            this.context = context;
        }

        @Override
        public void open() {
            i = 0;
            n = context.getValue(0).getLong();
        }

        @Override
        public List<Long> next() {
            if (i >= n)
                return null;
            return Arrays.asList(i++);
        }

        @Override
        public void close() {
        }
    }

    @Override
    public List<String> columnNames() {
        return NAMES;
    }

    @Override
    public int[] jdbcTypes()
    {
        return TYPES;
    }

    private static final List<String> NAMES = Arrays.asList("i");
    private static final int[] TYPES = new int[] { Types.INTEGER };
}
