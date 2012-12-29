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
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;
import com.akiban.server.types3.texpressions.TPreparedParameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BranchPlanGenerator
{
    private AkibanInformationSchema ais;
    private Schema schema;
    private Map<UserTable,Operator> plans = new HashMap<UserTable,Operator>();

    private static final Logger logger = LoggerFactory.getLogger(BranchPlanGenerator.class);

    public BranchPlanGenerator(AkibanInformationSchema ais) {
        this.ais = ais;
        this.schema = new Schema(ais);
    }

    public Schema getSchema() {
        return schema;
    }

    // TODO: Can narrow synchronization to plans and schema.
    public synchronized Operator generate(UserTable table) {
        Operator plan = plans.get(table);
        if (plan != null) return plan;

        PrimaryKey pkey = table.getPrimaryKey();
        UserTableRowType tableType = schema.userTableRowType(table);
        IndexRowType indexType = schema.indexRowType(pkey.getIndex());
        final int nkeys = indexType.nFields();

        List<TPreparedExpression> pexprs = new ArrayList<TPreparedExpression>(nkeys);
        for (int i = 0; i < nkeys; i++) {
            pexprs.add(new TPreparedParameter(i, indexType.typeInstanceAt(i)));
        }
        IndexBound bound = 
            new IndexBound(new RowBasedUnboundExpressions(indexType, null, pexprs),
                           new ColumnSelector() {
                               @Override
                               public boolean includesColumn(int columnPosition) {
                                   return columnPosition < nkeys;
                               }
                           });
        IndexKeyRange indexRange = IndexKeyRange.bounded(indexType,
                                                         bound, true,
                                                         bound, true);

        Ordering ordering = API.ordering(true);
        for (int i = 0; i < nkeys; i++) {
            ordering.append(null, 
                            new TPreparedField(indexType.typeInstanceAt(i), i), 
                            false);
        }

        Operator indexScan = API.indexScan_Default(indexType, indexRange, ordering,
                                                   true);
        plan = API.branchLookup_Default(indexScan, table.getGroup(), indexType,
                                        tableType, 
                                        API.InputPreservationOption.DISCARD_INPUT);
                                        
        if (logger.isDebugEnabled()) {
            logger.debug("Plan for {}:\n{}", table, 
                         com.akiban.util.Strings.join(new com.akiban.server.explain.format.DefaultFormatter(table.getName().getSchemaName(), true).format(plan.getExplainer(new com.akiban.server.explain.ExplainContext()))));
        }

        plans.put(table, plan);
        return plan;
    }

}
