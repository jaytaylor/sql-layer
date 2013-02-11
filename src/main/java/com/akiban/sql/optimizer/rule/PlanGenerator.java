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

package com.akiban.sql.optimizer.rule;

import static com.akiban.util.Strings.join;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.format.DefaultFormatter;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;
import com.akiban.server.types3.texpressions.TPreparedParameter;

/**
 * Generate one of a static set of internally used operator plans. 
 * @author tjoneslo
 *
 */
public class PlanGenerator {

    private static final Logger logger = LoggerFactory.getLogger(PlanGenerator.class);

    
    /**
     * Scan a group table and pull back all the rows from the group, with their children
     * Group table equivelent of a full table scan. 
     * 
     * Generates a plan
     * Filter_default 
     *   GroupScan (table group)
     *   
     *   
     */
    public static Operator generateScanPlan (AkibanInformationSchema ais, UserTable table) {
        final Schema schema = SchemaCache.globalSchema(ais);
        Operator plan = API.groupScan_Default(table.getGroup());
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

        return plan;
        
    }
    
    /**
     * Scan a group starting with Primary Key of a table, 
     * then get all of the children (if any). Primary key 
     * is set as parameters to the query context. 
     *  
     * Generates a plan:
     * Branch Lookup 
     *   Index Scan (table, pk-> ?[, ?...])
     */
    public static Operator generateBranchPlan (AkibanInformationSchema ais, UserTable table) {
        final Schema schema = SchemaCache.globalSchema(ais);
        final Operator indexScan = generateIndexScan (ais, table);
        final UserTableRowType tableType = schema.userTableRowType(table);

        PrimaryKey pkey = table.getPrimaryKeyIncludingInternal();
        IndexRowType indexType = schema.indexRowType(pkey.getIndex());
        
        Operator plan = API.branchLookup_Default(indexScan, table.getGroup(), indexType,
                                        tableType, 
                                        API.InputPreservationOption.DISCARD_INPUT);
                                        
        if (logger.isDebugEnabled()) {
            DefaultFormatter formatter = new DefaultFormatter(table.getName().getSchemaName());
            logger.debug("Branch Plan for {}:\n{}", table,
                         join(formatter.format(plan.getExplainer(new ExplainContext()))));
        }
        return plan;
    }
    
    /**
     * Scan a table starting with the primary key and return the full data row 
     * Generates a plan like
     * AncestorScan (Table)
     *   IndexScan (table, pk->?[, ?])
     */
    public static Operator generateAncestorPlan (AkibanInformationSchema ais, UserTable table) {
        final Schema schema = SchemaCache.globalSchema(ais);
        UserTableRowType tableType = schema.userTableRowType(table);

        List<UserTableRowType> ancestorType = new ArrayList<>(1);
        ancestorType.add (tableType);

        IndexRowType indexType = schema.indexRowType(table.getPrimaryKeyIncludingInternal().getIndex());
        
        Operator indexScan = generateIndexScan (ais, table);
        Operator lookup = API.ancestorLookup_Default(indexScan,
                table.getGroup(),
                indexType,
                ancestorType,
                API.InputPreservationOption.DISCARD_INPUT);
        if (logger.isDebugEnabled()) {
            DefaultFormatter formatter = new DefaultFormatter(table.getName().getSchemaName());
            logger.debug("Ancestor Plan for {}:\n{}", table,
                         join(formatter.format(lookup.getExplainer(new ExplainContext()))));
        }
        return lookup;
    }
    
    /**
     * Generate an index scan of the table based upon the table's primary key
     * Values for the scan are set as parameters in the PK order. 
     * @param ais
     * @param table
     * @return Operator plan for the Index scan 
     */
    private static Operator generateIndexScan (AkibanInformationSchema ais, UserTable table) {
        final Schema schema = SchemaCache.globalSchema(ais);
        PrimaryKey pkey = table.getPrimaryKeyIncludingInternal();
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

        return API.indexScan_Default(indexType, indexRange, ordering, true);
        
    }
}
