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

package com.akiban.sql.pg;

import com.akiban.qp.loadableplan.LoadableOperator;
import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.qp.loadableplan.LoadableDirectObjectPlan;
import com.akiban.server.types3.Types3Switch;

public class PostgresLoadablePlan
{
    public static PostgresStatement statement(PostgresServerSession server, 
                                              String planName, Object[] args) {
        LoadablePlan<?> loadablePlan = server.loadablePlan(planName);
        if (loadablePlan == null)
            return null;
        loadablePlan.ais(server.getAIS());
        if (loadablePlan instanceof LoadableOperator)
            return new PostgresLoadableOperator(
                    (LoadableOperator)loadablePlan,
                    args,
                    server.getBooleanProperty("newtypes", Types3Switch.ON));
        if (loadablePlan instanceof LoadableDirectObjectPlan)
            return new PostgresLoadableDirectObjectPlan((LoadableDirectObjectPlan)loadablePlan, args,
                    server.getBooleanProperty("newtypes", Types3Switch.ON));
        return null;
    }
    
    public static void setParameters(PostgresQueryContext context, Object[] args) {
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                context.setValue(i, args[i]);
            }
        }
    }

    // All static methods.
    private PostgresLoadablePlan() {
    }
}
