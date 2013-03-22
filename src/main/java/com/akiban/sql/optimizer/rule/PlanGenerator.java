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

package com.akiban.sql.optimizer.rule;

import static com.akiban.util.Strings.join;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
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
        final Operator indexScan = generateIndexScan(ais, table);
        final Schema schema = SchemaCache.globalSchema(ais);
        PrimaryKey pkey = table.getPrimaryKeyIncludingInternal();
        IndexRowType indexType = schema.indexRowType(pkey.getIndex());
        return generateBranchPlan(table, indexScan, indexType);
    }

    public static Operator generateBranchPlan (UserTable table, Operator scan, RowType scanType) {
        final Schema schema = (Schema)scanType.schema();
        final UserTableRowType tableType = schema.userTableRowType(table);
        Operator plan = API.branchLookup_Default(scan, table.getGroup(), 
                                                 scanType, tableType, 
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

        List<TPreparedExpression> pexprs = new ArrayList<>(nkeys);
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
