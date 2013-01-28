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
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.format.DefaultFormatter;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;
import com.akiban.server.types3.texpressions.TPreparedParameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.akiban.util.Strings.join;

public class PlanGenerator
{
    private AkibanInformationSchema ais;
    private Schema schema;
    private Map<UserTable,Operator> scanPlans = new HashMap<>();
    private Map<UserTable,Operator> branchPlans = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(PlanGenerator.class);

    public PlanGenerator(AkibanInformationSchema ais) {
        this.ais = ais;
        this.schema = SchemaCache.globalSchema(ais);
    }

    public Schema getSchema() {
        return schema;
    }

    // TODO: Can narrow synchronization to plans and schema.

    public synchronized Operator generateScanPlan(UserTable table) {
        Operator plan = scanPlans.get(table);
        if (plan != null) return plan;

        plan = API.groupScan_Default(table.getGroup());
        final List<RowType> keepTypes = new ArrayList<>();
        table.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable table) {
                keepTypes.add(schema.userTableRowType(table));
            }
        });
        plan = API.filter_Default(plan, keepTypes);

        if (logger.isDebugEnabled()) {
            DefaultFormatter formatter = new DefaultFormatter(table.getName().getSchemaName());
            logger.debug("Scan Plan for {}:\n{}", table, join(formatter.format(plan.getExplainer(new ExplainContext()))));

        }

        scanPlans.put(table, plan);
        return plan;
    }

    public synchronized Operator generateBranchPlan(UserTable table) {
        Operator plan = branchPlans.get(table);
        if (plan != null) return plan;

        PrimaryKey pkey = table.getPrimaryKey();
        final int nkeys = pkey.getColumns().size();
        UserTableRowType tableType = schema.userTableRowType(table);
        IndexRowType indexType = schema.indexRowType(pkey.getIndex());

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
            logger.debug("Branch Plan for {}:\n{}", table,
                         com.akiban.util.Strings.join(new com.akiban.server.explain.format.DefaultFormatter(table.getName().getSchemaName()).format(plan.getExplainer(new com.akiban.server.explain.ExplainContext()))));
        }

        branchPlans.put(table, plan);
        return plan;
    }

}
