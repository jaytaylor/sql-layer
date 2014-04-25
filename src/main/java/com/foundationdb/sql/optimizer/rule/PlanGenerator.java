/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.optimizer.rule;

import static com.foundationdb.util.Strings.join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.service.OverloadResolver;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.texpressions.TCastExpression;
import com.foundationdb.server.types.texpressions.TPreparedFunction;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;
import com.foundationdb.server.types.texpressions.TSequenceNextValueExpression;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.foundationdb.server.types.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.expression.RowBasedUnboundExpressions;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.texpressions.TPreparedParameter;

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
    public static Operator generateScanPlan (AkibanInformationSchema ais, Table table) {
        final Schema schema = SchemaCache.globalSchema(ais);
        Operator plan = API.groupScan_Default(table.getGroup());
        final List<RowType> keepTypes = new ArrayList<>();
        table.visit(new AbstractVisitor() {
            @Override
            public void visit(Table table) {
                keepTypes.add(schema.tableRowType(table));
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
    public static Operator generateBranchPlan (AkibanInformationSchema ais, Table table) {
        final Operator indexScan = generateIndexScan(ais, table);
        final Schema schema = SchemaCache.globalSchema(ais);
        PrimaryKey pkey = table.getPrimaryKeyIncludingInternal();
        IndexRowType indexType = schema.indexRowType(pkey.getIndex());
        return generateBranchPlan(table, indexScan, indexType);
    }

    public static Operator generateBranchPlan (Table table, Operator scan, RowType scanType) {
        final Schema schema = (Schema)scanType.schema();
        final TableRowType tableType = schema.tableRowType(table);
        final List<TableRowType> tableTypes = new ArrayList<>();
        tableTypes.add(tableType);
        for (RowType rowType : Schema.descendentTypes(tableType, schema.userTableTypes())) {
            tableTypes.add((TableRowType)rowType);
        }
        Operator plan = API.groupLookup_Default(scan, table.getGroup(), 
                                                scanType, tableTypes, 
                                                API.InputPreservationOption.DISCARD_INPUT, 1);
                                        
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
    public static Operator generateAncestorPlan (AkibanInformationSchema ais, Table table) {
        final Schema schema = SchemaCache.globalSchema(ais);
        TableRowType tableType = schema.tableRowType(table);

        List<TableRowType> ancestorType = new ArrayList<>(1);
        ancestorType.add (tableType);

        IndexRowType indexType = schema.indexRowType(table.getPrimaryKeyIncludingInternal().getIndex());
        
        Operator indexScan = generateIndexScan (ais, table);
        Operator lookup = API.groupLookup_Default(indexScan,
                table.getGroup(),
                indexType,
                ancestorType,
                API.InputPreservationOption.DISCARD_INPUT,
                1);
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
    private static Operator generateIndexScan (AkibanInformationSchema ais, Table table) {
        final Schema schema = SchemaCache.globalSchema(ais);
        PrimaryKey pkey = table.getPrimaryKeyIncludingInternal();
        final int nkeys = pkey.getColumns().size();
        IndexRowType indexType = schema.indexRowType(pkey.getIndex());

        List<TPreparedExpression> pexprs = new ArrayList<>(nkeys);
        for (int i = 0; i < nkeys; i++) {
            pexprs.add(new TPreparedParameter(i, indexType.typeAt(i)));
        }
        IndexBound bound = 
            new IndexBound(new RowBasedUnboundExpressions(indexType, pexprs),
                           new ColumnSelector() {
                               @Override
                               public boolean includesColumn(int columnPosition) {
                                   return columnPosition < nkeys;
                               }
                           });
        IndexKeyRange indexRange = IndexKeyRange.bounded(indexType,
                                                         bound, true,
                                                         bound, true);

        Ordering ordering = API.ordering();
        for (int i = 0; i < nkeys; i++) {
            ordering.append(new TPreparedField(indexType.typeAt(i), i),
                            false);
        }

        return API.indexScan_Default(indexType, indexRange, ordering);
    }

    /**
     * Generate an expression for a column's default value. {@code expression} may
     * be {@code null} if none was provided (e.g. from user).
     */
    public static TPreparedExpression generateDefaultExpression(Column column,
                                                                TPreparedExpression expression,
                                                                TypesRegistryService typesService,
                                                                TypesTranslator typesTranslator,
                                                                QueryContext queryContext) {
        Sequence sequence = column.getIdentityGenerator();
        if ((sequence != null) && Boolean.FALSE.equals(column.getDefaultIdentity())) {
            // FALSE => ALWAYS, override user value even if present
            expression = new TSequenceNextValueExpression(column.getType(), sequence);
        }
        else if (expression == null) {
            TInstance type = column.getType();
            if (sequence != null) {
                expression = new TSequenceNextValueExpression(type, sequence);
            }
            else if (column.getDefaultFunction() != null) {
                OverloadResolver<TValidatedScalar> resolver = typesService.getScalarsResolver();
                TValidatedScalar overload = resolver.get(column.getDefaultFunction(),
                                                         Collections.<TPreptimeValue>emptyList()).getOverload();
                TInstance dinst = overload.resultStrategy().fixed(false);
                TPreparedExpression defExpr = new TPreparedFunction(overload,
                                                                    dinst,
                                                                    Collections.<TPreparedExpression>emptyList());
                if (!dinst.equals(type)) {
                    TCast tcast = typesService.getCastsResolver().cast(dinst.typeClass(), type.typeClass());
                    defExpr = new TCastExpression(defExpr, tcast, type);
                }
                expression = defExpr;
            }
            else {
                final String defaultValue = column.getDefaultValue();
                final Value defaultValueSource;
                if(defaultValue == null) {
                    defaultValueSource = new Value(type);
                    defaultValueSource.putNull();
                } else {
                    TCast cast = type.typeClass().castFromVarchar();
                    if (cast != null) {
                        defaultValueSource = new Value(type);
                        TInstance valInst = typesTranslator.typeForString(defaultValue);
                        TExecutionContext executionContext = new TExecutionContext(Collections.singletonList(valInst),
                                                                                   type,
                                                                                   queryContext);
                        cast.evaluate(executionContext,
                                      new Value(valInst, defaultValue),
                                      defaultValueSource);
                    } else {
                        defaultValueSource = new Value(type, defaultValue);
                    }
                }
                expression = new TPreparedLiteral(type, defaultValueSource);
            }
        }
        return expression;
    }
}
