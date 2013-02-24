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

package com.akiban.server.service.externaldata;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;


import java.util.HashMap;
import java.util.Map;

public class PlanGenerator
{
    private Schema schema;
    private Map<UserTable,Operator> scanPlans = new HashMap<>();
    private Map<UserTable,Operator> branchPlans = new HashMap<>();

    public PlanGenerator(AkibanInformationSchema ais) {
        this.schema = SchemaCache.globalSchema(ais);
    }

    public Schema getSchema() {
        return schema;
    }

    // TODO: Can narrow synchronization to plans and schema.

    public synchronized Operator generateScanPlan(UserTable table) {
        Operator plan = scanPlans.get(table);
        if (plan != null) return plan;

        plan =  com.akiban.sql.optimizer.rule.PlanGenerator.generateScanPlan(schema.ais(), table);
        
        scanPlans.put(table, plan);
        return plan;
    }

    public synchronized Operator generateBranchPlan(UserTable table) {
        Operator plan = branchPlans.get(table);
        if (plan != null) return plan;
        
        plan =  com.akiban.sql.optimizer.rule.PlanGenerator.generateBranchPlan(schema.ais(), table);

        branchPlans.put(table, plan);
        return plan;
    }

}
