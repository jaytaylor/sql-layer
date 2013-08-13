/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.externaldata;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;


import java.util.HashMap;
import java.util.Map;

public class PlanGenerator
{
    private Schema schema;
    private Map<UserTable,Operator> scanPlans = new HashMap<>();
    private Map<UserTable,Operator> branchPlans = new HashMap<>();
    private Map<UserTable,Operator> ancestorPlans = new HashMap<>();

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
    
    public Operator generateAncestorPlan (UserTable table) {
        Operator plan = ancestorPlans.get(table);
        if (plan != null) return plan;
        
        plan = com.akiban.sql.optimizer.rule.PlanGenerator.generateAncestorPlan(schema.ais(), table);
        ancestorPlans.put(table, plan);
        return plan;
    }
}
