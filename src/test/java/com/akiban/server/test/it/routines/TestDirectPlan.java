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
