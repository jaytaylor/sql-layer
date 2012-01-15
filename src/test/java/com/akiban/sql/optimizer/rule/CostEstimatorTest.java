/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.ConstantExpression;
import com.akiban.sql.optimizer.plan.CostEstimate;
import com.akiban.sql.optimizer.plan.ExpressionNode;

import com.akiban.sql.optimizer.OptimizerTestBase;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.server.types.AkType;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.io.File;
import java.util.*;

public class CostEstimatorTest
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "costing");
    public static final String SCHEMA = OptimizerTestBase.DEFAULT_SCHEMA;

    protected AkibanInformationSchema ais;
    protected CostEstimator costEstimator;
    
    @Before
    public void loadSchema() throws Exception {
        ais = OptimizerTestBase.parseSchema(new File(RESOURCE_DIR, "schema.ddl"));
        OptimizerTestBase.loadGroupIndexes(ais, new File(RESOURCE_DIR, "group.idx"));
        costEstimator = new TestCostEstimator(ais, SCHEMA,
                                              new File(RESOURCE_DIR, "stats.yaml"));
    }

    protected static ExpressionNode constant(Object value, AkType type) {
        return new ConstantExpression(value, type);
    }

    @Test
    public void testSingleEquals() throws Exception {
        Index index = ais.getTable(SCHEMA, "items").getIndex("sku");
        List<ExpressionNode> equals = Collections.singletonList(constant("1234", AkType.VARCHAR));
        CostEstimate costEstimate = costEstimator.costIndexScan(index, equals,
                                                                null, false, null, false);
        assertEquals(100, costEstimate.getRowCount());
    }

}
