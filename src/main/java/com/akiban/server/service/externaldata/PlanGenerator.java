
package com.akiban.server.service.externaldata;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
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

    public Operator generateBranchPlan(UserTable table, Operator scan, RowType scanType) {
        // No caching possible.
        return com.akiban.sql.optimizer.rule.PlanGenerator.generateBranchPlan(table, scan, scanType);
    }

}
