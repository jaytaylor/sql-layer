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

import com.foundationdb.qp.row.ValuesRow;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.sql.optimizer.*;
import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.ExpressionsSource.DistinctState;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;
import com.foundationdb.sql.optimizer.plan.Sort.OrderByExpression;
import com.foundationdb.sql.optimizer.plan.UpdateStatement.UpdateColumn;

import com.foundationdb.sql.optimizer.rule.ExpressionAssembler.ColumnExpressionContext;
import com.foundationdb.sql.optimizer.rule.ExpressionAssembler.ColumnExpressionToIndex;
import com.foundationdb.sql.optimizer.rule.ExpressionAssembler.SubqueryOperatorAssembler;
import com.foundationdb.sql.optimizer.rule.range.ColumnRanges;
import com.foundationdb.sql.optimizer.rule.range.RangeSegment;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ParameterNode;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.API.InputPreservationOption;
import com.foundationdb.qp.operator.API.JoinType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TComparison;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.texpressions.AnySubqueryTExpression;
import com.foundationdb.server.types.texpressions.ExistsSubqueryTExpression;
import com.foundationdb.server.types.texpressions.ResultSetSubqueryTExpression;
import com.foundationdb.server.types.texpressions.ScalarSubqueryTExpression;
import com.foundationdb.server.types.texpressions.TCastExpression;
import com.foundationdb.server.types.texpressions.TNullExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.Value;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.UnsupportedSQLException;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.API.IntersectOption;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.*;

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.expression.RowBasedUnboundExpressions;
import com.foundationdb.qp.expression.UnboundExpressions;

import com.foundationdb.server.service.text.FullTextQueryBuilder;
import com.foundationdb.server.service.text.FullTextQueryExpression;

import com.foundationdb.server.explain.*;

import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.IndexRowPrefixSelector;
import com.foundationdb.util.tap.PointTap;
import com.foundationdb.util.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.util.*;

public class OperatorAssembler extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(OperatorAssembler.class);
    private static final PointTap SELECT_COUNT = Tap.createCount("sql: select");
    private static final PointTap INSERT_COUNT = Tap.createCount("sql: insert");
    private static final PointTap UPDATE_COUNT = Tap.createCount("sql: update");
    private static final PointTap DELETE_COUNT = Tap.createCount("sql: delete");
    public static final int CREATE_AS_BINDING_POSITION = 2;

    public static final int INSERTION_SORT_MAX_LIMIT = 100;

    public OperatorAssembler() {
    }
    
    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        new Assembler(plan).apply();
    }

    static class Assembler implements SubqueryOperatorAssembler {

        private final PlanContext planContext;
        private final SchemaRulesContext rulesContext;
        private final PlanExplainContext explainContext;
        private final Schema schema;
        private final ExpressionAssembler expressionAssembler;
        private final Set<Table> affectedTables;

        public Assembler(PlanContext planContext) {
            this.planContext = planContext;
            rulesContext = (SchemaRulesContext)planContext.getRulesContext();
            affectedTables = new HashSet<>();
            if (planContext instanceof ExplainPlanContext)
                explainContext = ((ExplainPlanContext)planContext).getExplainContext();
            else
                explainContext = null;
            schema = rulesContext.getSchema();
            expressionAssembler = new ExpressionAssembler(planContext);
            initializeBindings();
        }

        public void apply() {
            planContext.setPlan(assembleStatement((BaseStatement)planContext.getPlan()));
        }
        
        protected BasePlannable assembleStatement(BaseStatement plan) {
            if (plan instanceof SelectQuery) {
                SELECT_COUNT.hit();
                return selectQuery((SelectQuery)plan);
            } else if (plan instanceof DMLStatement) {
                return dmlStatement ((DMLStatement)plan);
            } else
                throw new UnsupportedSQLException("Cannot assemble plan: " + plan, null);
        }

        protected PhysicalSelect selectQuery(SelectQuery selectQuery) {
            PlanNode planQuery = selectQuery.getQuery();
            RowStream stream = assembleQuery(planQuery);
            List<PhysicalResultColumn> resultColumns;
            if (planQuery instanceof ResultSet) {
                List<ResultField> results = ((ResultSet)planQuery).getFields();
                resultColumns = getResultColumns(results);
            }
            else {
                // VALUES results in column1, column2, ...
                resultColumns = getResultColumns(stream.rowType.nFields());
            }
            if (explainContext != null)
                explainSelectQuery(stream.operator, selectQuery);
            return new PhysicalSelect(stream.operator, stream.rowType, resultColumns, 
                                      getParameterTypes(), 
                                      selectQuery.getCostEstimate(),
                                      affectedTables);
        }

        protected void explainSelectQuery(Operator plan, SelectQuery selectQuery) {
            Attributes atts = new Attributes();
            explainCostEstimate(atts, selectQuery.getCostEstimate());
            explainContext.putExtraInfo(plan, new CompoundExplainer(Type.EXTRA_INFO, atts));
        }

        protected PhysicalUpdate dmlStatement (DMLStatement statement) {
            
            PlanNode planQuery = statement.getInput();
            RowStream stream = assembleStream(planQuery);
            
            //If we're returning results we need the resultColumns,
            // including names and types for returning to the user.
            List<PhysicalResultColumn> resultColumns = null;
            if (statement.getResultField() != null) {
                resultColumns = getResultColumns(statement.getResultField());
            }
            // Returning rows, if the table is not null, the insert is returning rows 
            // which need to be passed to the user. 
            boolean returning = (statement.getReturningTable() != null);
            return new PhysicalUpdate(stream.operator, getParameterTypes(),
                                      stream.rowType,
                                      resultColumns,
                                      returning,
                                      returning || !isBulkInsert(planQuery),
                                      statement.getCostEstimate(),
                                      affectedTables);
        }

        protected RowStream assembleInsertStatement (InsertStatement insert) {
            INSERT_COUNT.hit();
            
            PlanNode planQuery = insert.getInput();
            List<ExpressionNode> projectFields = null;
            if (planQuery instanceof Project) {
                Project project = (Project)planQuery;
                projectFields = project.getFields();
                planQuery = project.getInput();
            }
            RowStream stream = assembleQuery(planQuery);
            
            stream = assembleInsertProjectTable (stream, projectFields, insert);

            stream.operator = API.insert_Returning(stream.operator);
            
            if (explainContext != null)
                explainInsertStatement(stream.operator, insert);
            
            return stream;
        }
        
        protected RowStream assembleInsertProjectTable (RowStream input, 
                List<ExpressionNode> projectFields, InsertStatement insert) {

            TableRowType targetRowType =
                    tableRowType(insert.getTargetTable());
            Table table = insert.getTargetTable().getTable();

            List<TPreparedExpression> insertsP = null;
            if (projectFields != null) {
                // In the common case, we can project into a wider row
                // of the correct type directly.
                insertsP = assembleExpressions(projectFields, input.fieldOffsets);
            }
            else {
                // VALUES just needs each field, which will get rearranged below.
                int nfields = input.rowType.nFields();
                insertsP = new ArrayList<>(nfields);
                for (int i = 0; i < nfields; ++i) {
                    insertsP.add(new TPreparedField(input.rowType.typeAt(i), i));
                }
            }

            TPreparedExpression[] row = new TPreparedExpression[targetRowType.nFields()];
            int ncols = insertsP.size();
            for (int i = 0; i < ncols; i++) {
                Column column = insert.getTargetColumns().get(i);
                TInstance type = column.getType();
                int pos = column.getPosition();
                row[pos] = insertsP.get(i);
                
                if (!type.equals(row[pos].resultType())) {
                    TypesRegistryService registry = rulesContext.getTypesRegistry();
                    TCast tcast = registry.getCastsResolver().cast(type.typeClass(), row[pos].resultType().typeClass());
                    row[pos] = new TCastExpression(row[pos], tcast, type);
                }
            }
            // Fill in column default values
            for (int i = 0, len = targetRowType.nFields(); i < len; ++i) {
                Column column = table.getColumnsIncludingInternal().get(i);
                row[i] = expressionAssembler.assembleColumnDefault(column, row[i]);
            }
            
            insertsP = Arrays.asList(row); // Now complete row.
            input.operator = API.project_Table(input.operator, input.rowType,
                    targetRowType, insertsP);
            input.rowType = input.operator.rowType();
            input.fieldOffsets = new ColumnSourceFieldOffsets(insert.getTable(), targetRowType);
            return input;
        }

        protected void explainInsertStatement(Operator plan, InsertStatement insertStatement) {
            Attributes atts = new Attributes();
            TableName tableName = insertStatement.getTargetTable().getTable().getName();
            atts.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(tableName.getSchemaName()));
            atts.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(tableName.getTableName()));
            for (Column column : insertStatement.getTargetColumns()) {
                atts.put(Label.COLUMN_NAME, PrimitiveExplainer.getInstance(column.getName()));
            }
            explainContext.putExtraInfo(plan, new CompoundExplainer(Type.EXTRA_INFO, atts));
        }

        protected boolean isBulkInsert(PlanNode planQuery) {
            if (!(planQuery instanceof InsertStatement))
                return false;
            PlanNode insertSource = ((InsertStatement)planQuery).getInput();
            if (!(insertSource instanceof ExpressionsSource))
                return false;
            for (List<ExpressionNode> exprs : ((ExpressionsSource)insertSource).getExpressions()) {
                if (exprs.isEmpty()) return false; // Must want just generated columns.
                for (ExpressionNode expr : exprs) {
                    if (!(expr instanceof ConstantExpression)) {
                        return false;
                    }
                }
            }
            return true;
        }

        protected RowStream assembleUpdateStatement (UpdateStatement updateStatement) {
            UPDATE_COUNT.hit();
            PlanNode input = updateStatement.getInput();
            RowStream stream = assembleQuery(input);
            TableRowType targetRowType = tableRowType(updateStatement.getTargetTable());
            if (input instanceof NullSource)
                stream.rowType = targetRowType;
            else
                assert (stream.rowType == targetRowType) : input;

            List<UpdateColumn> updateColumns = updateStatement.getUpdateColumns();
            List<TPreparedExpression> updatesP = assembleUpdates(targetRowType, updateColumns,
                    stream.fieldOffsets);
            UpdateFunction updateFunction = 
                new ExpressionRowUpdateFunction(updatesP, targetRowType);

            stream.operator = API.update_Returning(stream.operator, updateFunction);
            stream.fieldOffsets = new ColumnSourceFieldOffsets (updateStatement.getTable(), targetRowType);
            if (explainContext != null)
                explainUpdateStatement(stream.operator, updateStatement, updateColumns, updatesP);            
            return stream;
        }

        protected void explainUpdateStatement(Operator plan, UpdateStatement updateStatement, List<UpdateColumn> updateColumns, List<TPreparedExpression> updatesP) {
            Attributes atts = new Attributes();
            TableName tableName = updateStatement.getTargetTable().getTable().getName();
            atts.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(tableName.getSchemaName()));
            atts.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(tableName.getTableName()));
            for (UpdateColumn column : updateColumns) {
                atts.put(Label.COLUMN_NAME, PrimitiveExplainer.getInstance(column.getColumn().getName()));
                atts.put(Label.EXPRESSIONS, updatesP.get(column.getColumn().getPosition()).getExplainer(explainContext));
            }
            explainContext.putExtraInfo(plan, new CompoundExplainer(Type.EXTRA_INFO, atts));
        }
      
        protected RowStream assembleDeleteStatement (DeleteStatement delete) {
            DELETE_COUNT.hit();
            RowStream stream = assembleQuery(delete.getInput());
            
            TableRowType targetRowType = tableRowType(delete.getTargetTable());
            
            stream.operator = API.delete_Returning(stream.operator, false);
            stream.fieldOffsets = new ColumnSourceFieldOffsets(delete.getTable(), targetRowType);
            
            if (explainContext != null)
                explainDeleteStatement(stream.operator, delete);
            
            return stream;
        
        }

        protected RowStream assembleCreateAsTemp( CreateAs createAs) {
            RowStream stream = new RowStream();
            TableSource tableSource = createAs.getTableSource();
            TableRowType rowType = tableRowType(tableSource);
            com.foundationdb.server.types.value.Value values[] = new com.foundationdb.server.types.value.Value[rowType.nFields()];
            for(int i = 0; i < rowType.nFields(); i++){
                values[i] = new Value(rowType.typeAt(i));
                values[i].putNull();
            }
            ValuesRow valuesRow = new ValuesRow(rowType, values);
            Collection<BindableRow> bindableRows = new ArrayList<>();
            bindableRows.add(BindableRow.of(valuesRow));

            stream.operator = API.emitBoundRow_Nested(
                    API.valuesScan_Default(bindableRows, rowType),
                    rowType,
                    rowType,
                    rowType,
                    CREATE_AS_BINDING_POSITION);
            stream.rowType = rowType;
            return stream;
        }

        protected void explainDeleteStatement(Operator plan, DeleteStatement deleteStatement) {
            Attributes atts = new Attributes();
            TableName tableName = deleteStatement.getTargetTable().getTable().getName();
            atts.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(tableName.getSchemaName()));
            atts.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(tableName.getTableName()));
            explainContext.putExtraInfo(plan, new CompoundExplainer(Type.EXTRA_INFO, atts));
        }
        
        protected RowStream assembleUpdateInput(UpdateInput updateInput) {
            RowStream stream = assembleQuery(updateInput.getInput());
            TableSource table = updateInput.getTable();
            TableRowType rowType = tableRowType(table);
            if ((stream.rowType != rowType) ||
                !boundRowIsForTable(stream.fieldOffsets, table)) {
                ColumnExpressionToIndex boundRow = lookupNestedBoundRow(table);
                stream.operator = API.emitBoundRow_Nested(stream.operator,
                                                          stream.rowType,
                                                          rowType,
                                                          boundRow.getRowType(),
                                                          getBindingPosition(boundRow));
                stream.rowType = rowType;
                stream.fieldOffsets = new ColumnSourceFieldOffsets(table, stream.rowType);
            }
            return stream;
        }

       // Assemble the top-level query. If there is a ResultSet at
        // the top, it is not handled here, since its meaning is
        // different for the different statement types.
        protected RowStream assembleQuery(PlanNode planQuery) {
            if (planQuery instanceof ResultSet)
                planQuery = ((ResultSet)planQuery).getInput();
            return assembleStream(planQuery);
        }

        // Assemble an ordinary stream node.
        protected RowStream assembleStream(PlanNode node) {
            if (node instanceof IndexScan)
                return assembleIndexScan((IndexScan) node);
            else if (node instanceof GroupScan)
                return assembleGroupScan((GroupScan) node);
            else if (node instanceof Select)
                return assembleSelect((Select) node);
            else if (node instanceof Flatten)
                return assembleFlatten((Flatten) node);
            else if (node instanceof AncestorLookup)
                return assembleAncestorLookup((AncestorLookup) node);
            else if (node instanceof BranchLookup)
                return assembleBranchLookup((BranchLookup) node);
            else if (node instanceof MapJoin)
                return assembleMapJoin((MapJoin) node);
            else if (node instanceof Product)
                return assembleProduct((Product) node);
            else if (node instanceof AggregateSource)
                return assembleAggregateSource((AggregateSource) node);
            else if (node instanceof Distinct)
                return assembleDistinct((Distinct) node);
            else if (node instanceof Sort)
                return assembleSort((Sort) node);
            else if (node instanceof Limit)
                return assembleLimit((Limit) node);
            else if (node instanceof NullIfEmpty)
                return assembleNullIfEmpty((NullIfEmpty) node);
            else if (node instanceof OnlyIfEmpty)
                return assembleOnlyIfEmpty((OnlyIfEmpty) node);
            else if (node instanceof Project)
                return assembleProject((Project) node);
            else if (node instanceof ExpressionsSource)
                return assembleExpressionsSource((ExpressionsSource) node);
            else if (node instanceof SubquerySource)
                return assembleSubquerySource((SubquerySource) node);
            else if (node instanceof NullSource)
                return assembleNullSource((NullSource) node);
            else if (node instanceof UsingBloomFilter)
                return assembleUsingBloomFilter((UsingBloomFilter) node);
            else if (node instanceof BloomFilterFilter)
                return assembleBloomFilterFilter((BloomFilterFilter) node);
            else if (node instanceof UsingHashTable)
                return assembleUsingHashTable((UsingHashTable)node);
            else if (node instanceof HashTableLookup)
                return assembleHashTableLookup((HashTableLookup)node);
            else if (node instanceof FullTextScan)
                return assembleFullTextScan((FullTextScan) node);
            else if (node instanceof InsertStatement) 
                return assembleInsertStatement((InsertStatement)node);
            else if (node instanceof DeleteStatement)
                return assembleDeleteStatement((DeleteStatement)node);
            else if (node instanceof UpdateStatement)
                return assembleUpdateStatement((UpdateStatement)node);
            else if (node instanceof UpdateInput)
                return assembleUpdateInput((UpdateInput)node);
            else if (node instanceof Buffer)
                return assembleBuffer((Buffer)node);
            else if (node instanceof ExpressionsHKeyScan)
                return assembleExpressionsHKeyScan((ExpressionsHKeyScan) node);
            else if (node instanceof CreateAs)
                return assembleCreateAsTemp((CreateAs)node);
            else if (node instanceof SetPlanNode) {
                SetPlanNode setPlan = (SetPlanNode)node;
                switch (setPlan.getOperationType()) {
                    case INTERSECT:
                        return assembleIntersect(setPlan);
                    case EXCEPT:
                        return assembleExcept(setPlan);
                    case UNION:
                        return assembleUnion(setPlan);
                    default:
                        throw new UnsupportedSQLException("Set operation node without type " + node, null);
                }
            }
            else
                throw new UnsupportedSQLException("Plan node " + node, null);
        }
        
        protected enum IntersectionMode { NONE, OUTPUT, SELECT };

        protected RowStream assembleIndexScan(IndexScan index) {
            return assembleIndexScan(index, IntersectionMode.NONE, useSkipScan(index));
        }

        protected RowStream assembleIndexScan(IndexScan index, IntersectionMode forIntersection, boolean useSkipScan) {
            if (index instanceof SingleIndexScan)
                return assembleSingleIndexScan((SingleIndexScan) index, forIntersection);
            else if (index instanceof MultiIndexIntersectScan)
                return assembleIndexIntersection((MultiIndexIntersectScan) index, forIntersection, useSkipScan);
            else
                throw new UnsupportedSQLException("Plan node " + index, null);
        }

        private RowStream assembleIndexIntersection(MultiIndexIntersectScan index, IntersectionMode forIntersection, boolean useSkipScan) {
            RowStream stream = new RowStream();
            RowStream outputScan = assembleIndexScan(index.getOutputIndexScan(), 
                                                     (forIntersection == IntersectionMode.SELECT) ? IntersectionMode.SELECT : IntersectionMode.OUTPUT, useSkipScan);
            RowStream selectorScan = assembleIndexScan(index.getSelectorIndexScan(), 
                                                       IntersectionMode.SELECT, useSkipScan);

            RowType selectorRowType = selectorScan.rowType;
            RowType outputRowType = outputScan.rowType;
 
            int nFieldsToCompare = index.getComparisonFields();
 
            List<TComparison> comparisons = new ArrayList<>(nFieldsToCompare);
            TypesRegistryService reg = rulesContext.getTypesRegistry();

            // Intersect can use raw value compare if the comparisons list is null (i.e. all types the same)
            boolean anySet = false;
            for (int n = 0; n < nFieldsToCompare; ++n)
            {
                TClass left = selectorRowType.typeAt(index.getSelectorIndexScan().getPeggedCount() + n).typeClass();
                TClass right = outputRowType.typeAt(index.getOutputIndexScan().getPeggedCount() + n).typeClass();
                if (left != right) {
                    anySet = true;
                    comparisons.add(n, reg.getKeyComparable(left, right).getComparison());
                } else {
                    comparisons.add(n, null);
                }
            }
            
            stream.operator = API.intersect_Ordered(
                    outputScan.operator,
                    selectorScan.operator,
                    outputRowType,
                    selectorRowType,
                    index.getOutputOrderingFields(),
                    index.getSelectorOrderingFields(),
                    index.getComparisonFieldDirections(),
                    JoinType.INNER_JOIN,
                    (useSkipScan) ?
                            EnumSet.of(API.IntersectOption.OUTPUT_LEFT,
                                    API.IntersectOption.SKIP_SCAN) :
                            EnumSet.of(API.IntersectOption.OUTPUT_LEFT,
                                    API.IntersectOption.SEQUENTIAL_SCAN),
                    anySet ? comparisons : null,
                    true);
            stream.rowType = outputScan.rowType;
            stream.fieldOffsets = new IndexFieldOffsets(index, stream.rowType);

            return stream;
        }

        protected RowStream assembleSingleIndexScan(SingleIndexScan indexScan, IntersectionMode forIntersection) {
            RowStream stream = new RowStream();
            Index index = indexScan.getIndex();
            IndexRowType indexRowType = schema.indexRowType(index);
            IndexScanSelector selector;
            if (index.isTableIndex()) {
                selector = IndexScanSelector.inner(index);
            }
            else {
                switch (index.getJoinType()) {
                case LEFT:
                    selector = IndexScanSelector
                        .leftJoinAfter(index, 
                                       indexScan.getLeafMostInnerTable().getTable().getTable());
                    break;
                case RIGHT:
                    selector = IndexScanSelector
                        .rightJoinUntil(index, 
                                        indexScan.getRootMostInnerTable().getTable().getTable());
                    break;
                default:
                    throw new AkibanInternalException("Unknown index join type " +
                                                      index);
                }
            }
            if (index.isSpatial()) {
                stream.operator = API.indexScan_Default(indexRowType,
                                                        assembleSpatialIndexKeyRange(indexScan, null),
                                                        API.ordering(), // TODO: what ordering?
                                                        selector,
                                                        rulesContext.getPipelineConfiguration().getIndexScanLookaheadQuantum());
                indexRowType = indexRowType.physicalRowType();
                stream.rowType = indexRowType;
            }
            else if (indexScan.getConditionRange() == null) {
                stream.operator = API.indexScan_Default(indexRowType,
                                                        assembleIndexKeyRange(indexScan, null),
                                                        assembleIndexOrdering(indexScan, indexRowType),
                                                        selector,
                                                        rulesContext.getPipelineConfiguration().getIndexScanLookaheadQuantum());
                stream.rowType = indexRowType;
            }
            else {
                ColumnRanges range = indexScan.getConditionRange();
                // Non-single-point ranges are ordered by the ranges
                // themselves as part of merging segments, so that index
                // column is an ordering column.
                // Single-point ranges have only one value for the range column,
                // so they can order by the following columns if we're
                // willing to do the more expensive ordered union.
                // Determine whether anything is taking advantage of this:
                // * Index is being intersected.
                // * Index is effective for query ordering.
                // ** See also special case in AggregateSplitter.directIndexMinMax().
                boolean unionOrdered = false, unionOrderedAll = false;
                if (range.isAllSingle()) {
                    if (forIntersection != IntersectionMode.NONE) {
                        unionOrdered = true;
                        if (forIntersection == IntersectionMode.OUTPUT) {
                            unionOrderedAll = true;
                        }
                    }
                    else if (indexScan.getOrderEffectiveness() != IndexScan.OrderEffectiveness.NONE) {
                        unionOrderedAll = unionOrdered = true;
                    }
                }
                for (RangeSegment rangeSegment : range.getSegments()) {
                    Operator scan = API.indexScan_Default(indexRowType,
                                                          assembleIndexKeyRange(indexScan, null, rangeSegment),
                                                          assembleIndexOrdering(indexScan, indexRowType),
                                                          selector,
                                                          rulesContext.getPipelineConfiguration().getIndexScanLookaheadQuantum());
                    if (stream.operator == null) {
                        stream.operator = scan;
                        stream.rowType = indexRowType;
                    }
                    else if (unionOrdered) {
                        int nequals = indexScan.getNEquality();
                        List<OrderByExpression> ordering = indexScan.getOrdering();
                        int nordering = ordering.size() - nequals;
                        boolean[] ascending = new boolean[nordering];
                        for (int i = 0; i < nordering; i++) {
                            ascending[i] = ordering.get(nequals + i).isAscending();
                        }
                        stream.operator = API.union_Ordered(stream.operator, scan,
                                                            (IndexRowType)stream.rowType, indexRowType,
                                                            nordering, nordering, 
                                                            ascending, unionOrderedAll);
                    }
                    else {
                        stream.operator = API.unionAll_Default(stream.operator, stream.rowType, scan, indexRowType, rulesContext.getPipelineConfiguration().isUnionAllOpenBoth());
                        stream.rowType = stream.operator.rowType();
                    }
                }
                if (stream.operator == null) {
                    stream.operator = API.valuesScan_Default(Collections.<BindableRow>emptyList(), 
                                                             indexRowType);
                    stream.rowType = indexRowType;
                }
            }
            stream.fieldOffsets = new IndexFieldOffsets(indexScan, indexRowType);
            if (explainContext != null)
                explainSingleIndexScan(stream.operator, indexScan, index);
            return stream;
        }

        protected void explainSingleIndexScan(Operator operator, SingleIndexScan indexScan, Index index) {
            Attributes atts = new Attributes();
            atts.put(Label.ORDER_EFFECTIVENESS, PrimitiveExplainer.getInstance(indexScan.getOrderEffectiveness().name()));
            atts.put(Label.USED_COLUMNS, PrimitiveExplainer.getInstance(indexScan.usesAllColumns() ? indexScan.getColumns().size() : indexScan.getNKeyColumns()));
            explainCostEstimate(atts, indexScan.getScanCostEstimate());
            explainContext.putExtraInfo(operator, new CompoundExplainer(Type.EXTRA_INFO, atts));
        }

        protected void explainCostEstimate(Attributes atts, CostEstimate costEstimate) {
            if (costEstimate != null)
                atts.put(Label.COST, PrimitiveExplainer.getInstance(costEstimate.toString()));
        }

        /**
         * If there are this many or more scans feeding into a tree
         * of intersection / union, then skip scan is enabled for it.
         * (3 scans means 2 intersections or intersection with a two-value union.)
         */
        public static final int SKIP_SCAN_MIN_COUNT_DEFAULT = 3;

        protected boolean useSkipScan(IndexScan index) {
            if (!(index instanceof MultiIndexIntersectScan))
                return false;
            int count = countScans(index);
            int minCount;
            String prop = rulesContext.getProperty("skipScanMinCount");
            if (prop != null)
                minCount = Integer.valueOf(prop);
            else
                minCount = SKIP_SCAN_MIN_COUNT_DEFAULT;
            return (count >= minCount);
        }

        private int countScans(IndexScan index) {
            if (index instanceof SingleIndexScan) {
                SingleIndexScan sindex = (SingleIndexScan)index;
                if (sindex.getConditionRange() == null)
                    return 1;
                else
                    return sindex.getConditionRange().getSegments().size();
            }
            else if (index instanceof MultiIndexIntersectScan) {
                MultiIndexIntersectScan mindex = (MultiIndexIntersectScan)index;
                return countScans(mindex.getOutputIndexScan()) +
                       countScans(mindex.getSelectorIndexScan());
            }
            else
                return 0;
        }

        protected RowStream assembleGroupScan(GroupScan groupScan) {
            RowStream stream = new RowStream();
            Group group = groupScan.getGroup().getGroup();
            stream.operator = API.groupScan_Default(group);
            stream.unknownTypesPresent = true;
            return stream;
        }

        protected RowStream assembleExpressionsSource(ExpressionsSource expressionsSource) {
            RowStream stream = new RowStream();
            stream.rowType = valuesRowType(expressionsSource);
            List<BindableRow> bindableRows = new ArrayList<>();
            for (List<ExpressionNode> exprs : expressionsSource.getExpressions()) {
                List<TPreparedExpression> tExprs = assembleExpressions(exprs, stream.fieldOffsets);
                bindableRows.add(BindableRow.of(stream.rowType, tExprs, planContext.getQueryContext()));
            }
            stream.operator = API.valuesScan_Default(bindableRows, stream.rowType);
            stream.fieldOffsets = new ColumnSourceFieldOffsets(expressionsSource, 
                                                               stream.rowType);
            if (expressionsSource.getDistinctState() == DistinctState.NEED_DISTINCT) {
                // Add Sort (usually _InsertionLimited) and Distinct.
                assembleSort(stream, stream.rowType.nFields(), expressionsSource,
                             API.SortOption.SUPPRESS_DUPLICATES);
            }
            return stream;
        }

        protected RowStream assembleExpressionsHKeyScan(ExpressionsHKeyScan node) {
            RowStream stream = new RowStream();
            stream.rowType = schema.newHKeyRowType(node.getHKey());
            List<TPreparedExpression> keys = assembleExpressions(node.getKeys(), stream.fieldOffsets);
            stream.operator = API.hKeyRow_Default(stream.rowType, keys);
            return stream;
        }

        protected RowStream assembleSubquerySource(SubquerySource subquerySource) {
            PlanNode subquery = subquerySource.getSubquery().getQuery();
            if (subquery instanceof ResultSet)
                subquery = ((ResultSet)subquery).getInput();
            RowStream stream = assembleStream(subquery);
            stream.fieldOffsets = new ColumnSourceFieldOffsets(subquerySource, 
                                                               stream.rowType);
            return stream;
        }

        protected RowStream assembleNullSource(NullSource node) {
            return assembleExpressionsSource(new ExpressionsSource(Collections.<List<ExpressionNode>>emptyList()));
        }

        protected RowStream assembleSelect(Select select) {
            RowStream stream = assembleStream(select.getInput());
            ConditionDependencyAnalyzer dependencies = null;
            for (ConditionExpression condition : select.getConditions()) {
                RowType rowType = stream.rowType;
                ColumnExpressionToIndex fieldOffsets = stream.fieldOffsets;
                if (rowType == null) {
                    // Pre-flattening case: get the single table this
                    // condition must have come from and use its row-type.
                    // TODO: Would it be better if earlier rule saved this?
                    if (dependencies == null)
                        dependencies = new ConditionDependencyAnalyzer(select);
                    TableSource table = (TableSource)dependencies.analyze(condition);
                    rowType = tableRowType(table);
                    fieldOffsets = new ColumnSourceFieldOffsets(table, rowType);
                }
                stream.operator = API.select_HKeyOrdered(stream.operator,
                        rowType,
                        assembleExpression(condition,
                                fieldOffsets));
            }
            return stream;
        }

        protected RowStream assembleFlatten(Flatten flatten) {
            RowStream stream = assembleStream(flatten.getInput());
            List<TableNode> tableNodes = flatten.getTableNodes();
            TableNode tableNode = tableNodes.get(0);
            RowType tableRowType = tableRowType(tableNode);
            stream.rowType = tableRowType;
            int ntables = tableNodes.size();
            if (ntables == 1) {
                TableSource tableSource = flatten.getTableSources().get(0);
                if (tableSource != null)
                    stream.fieldOffsets = new ColumnSourceFieldOffsets(tableSource, 
                                                                       tableRowType);
            }
            else {
                Flattened flattened = new Flattened();
                flattened.addTable(tableRowType, flatten.getTableSources().get(0));
                for (int i = 1; i < ntables; i++) {
                    tableNode = tableNodes.get(i);
                    tableRowType = tableRowType(tableNode);
                    flattened.addTable(tableRowType, flatten.getTableSources().get(i));
                    API.JoinType flattenType = null;
                    switch (flatten.getJoinTypes().get(i-1)) {
                    case INNER:
                        flattenType = API.JoinType.INNER_JOIN;
                        break;
                    case LEFT:
                        flattenType = API.JoinType.LEFT_JOIN;
                        break;
                    case RIGHT:
                        flattenType = API.JoinType.RIGHT_JOIN;
                        break;
                    case FULL_OUTER:
                        flattenType = API.JoinType.FULL_JOIN;
                        break;
                    }
                    stream.operator = API.flatten_HKeyOrdered(stream.operator, 
                                                              stream.rowType,
                                                              tableRowType,
                                                              flattenType);
                    stream.rowType = stream.operator.rowType();
                }
                flattened.setRowType(stream.rowType);
                stream.fieldOffsets = flattened;
            }
            if (stream.unknownTypesPresent) {
                stream.operator = API.filter_Default(stream.operator,
                                                     Collections.singletonList(stream.rowType));
                stream.unknownTypesPresent = false;
            }
            return stream;
        }

        protected RowStream assembleAncestorLookup(AncestorLookup ancestorLookup) {
            RowStream stream;
            Group group = ancestorLookup.getDescendant().getGroup();
            List<TableRowType> outputRowTypes =
                new ArrayList<>(ancestorLookup.getAncestors().size());
            for (TableNode table : ancestorLookup.getAncestors()) {
                outputRowTypes.add(tableRowType(table));
            }
            PlanNode input = ancestorLookup.getInput();
            if (input instanceof GroupLoopScan) {
                stream = new RowStream();
                ColumnExpressionToIndex boundRow = lookupNestedBoundRow(((GroupLoopScan)ancestorLookup.getInput()));
                stream.operator = API.ancestorLookup_Nested(group,
                                                            boundRow.getRowType(),
                                                            outputRowTypes,
                                                            getBindingPosition(boundRow),
                                                            rulesContext.getPipelineConfiguration().getGroupLookupLookaheadQuantum());
            }
            else {
                BranchLookup branchLookup = null;
                if (input instanceof BranchLookup) {
                    branchLookup = (BranchLookup)input;
                    if ((branchLookup.getInput() == null) ||
                        (branchLookup.getSource().getGroup() != group)) {
                        branchLookup = null;
                    }
                }
                if (branchLookup != null) {
                    for (TableSource table : branchLookup.getTables()) {
                        outputRowTypes.add(tableRowType(table));
                    }
                    stream = assembleStream(branchLookup.getInput());
                    stream.unknownTypesPresent = true;
                }
                else
                    stream = assembleStream(input);
                RowType inputRowType = stream.rowType; // The index row type.
                API.InputPreservationOption flag = API.InputPreservationOption.DISCARD_INPUT;
                if (!isIndexRowType(inputRowType)) {
                    // Getting from branch lookup.
                    inputRowType = tableRowType(ancestorLookup.getDescendant());
                    flag = API.InputPreservationOption.KEEP_INPUT;
                }
                stream.operator = API.groupLookup_Default(stream.operator,
                                                          group,
                                                          inputRowType,
                                                          outputRowTypes,
                                                          flag,
                                                          rulesContext.getPipelineConfiguration().getGroupLookupLookaheadQuantum());
            }
            stream.rowType = null;
            stream.fieldOffsets = null;
            return stream;
        }

        protected RowStream assembleBranchLookup(BranchLookup branchLookup) {
            RowStream stream;
            Group group = branchLookup.getSource().getGroup();
            List<TableRowType> outputRowTypes =
                new ArrayList<>(branchLookup.getTables().size());
            if (false)      // TODO: Any way to check that this matched?
                outputRowTypes.add(tableRowType(branchLookup.getBranch()));
            for (TableSource table : branchLookup.getTables()) {
                outputRowTypes.add(tableRowType(table));
            }
            if (branchLookup.getInput() == null) {
                // Simple version for Product_Nested.
                stream = new RowStream();
                API.InputPreservationOption flag = API.InputPreservationOption.KEEP_INPUT;
                ColumnExpressionToIndex boundRow = boundRows.peek();
                stream.operator = API.branchLookup_Nested(group,
                                                          boundRow.getRowType(),
                                                          tableRowType(branchLookup.getSource()),
                                                          tableRowType(branchLookup.getAncestor()),
                                                          outputRowTypes, 
                                                          flag,
                                                          getBindingPosition(boundRow),
                                                          rulesContext.getPipelineConfiguration().getGroupLookupLookaheadQuantum());
                
            }
            else if (branchLookup.getInput() instanceof GroupLoopScan) {
                // Fuller version for group join across subquery boundary.
                stream = new RowStream();
                API.InputPreservationOption flag = API.InputPreservationOption.DISCARD_INPUT;
                ColumnExpressionToIndex boundRow = lookupNestedBoundRow(((GroupLoopScan)branchLookup.getInput()));
                stream.operator = API.branchLookup_Nested(group,
                                                          boundRow.getRowType(),
                                                          boundRow.getRowType(),
                                                          tableRowType(branchLookup.getAncestor()),
                                                          outputRowTypes, 
                                                          flag,
                                                          getBindingPosition(boundRow),
                                                          rulesContext.getPipelineConfiguration().getGroupLookupLookaheadQuantum());
            }
            else {
                // Ordinary inline version.
                stream = assembleStream(branchLookup.getInput());
                RowType inputRowType = stream.rowType; // The index row type.
                API.InputPreservationOption flag = API.InputPreservationOption.DISCARD_INPUT;
                if (!isIndexRowType(inputRowType)) {
                    // Getting from ancestor lookup.
                    inputRowType = tableRowType(branchLookup.getSource());
                    flag = API.InputPreservationOption.KEEP_INPUT;
                }
                stream.operator = API.groupLookup_Default(stream.operator,
                                                          group,
                                                          inputRowType,
                                                          outputRowTypes, 
                                                          flag,
                                                          rulesContext.getPipelineConfiguration().getGroupLookupLookaheadQuantum());
            }
            stream.rowType = null;
            stream.unknownTypesPresent = true;
            stream.fieldOffsets = null;
            return stream;
        }

        protected static boolean isIndexRowType(RowType rowType) {
            return ((rowType instanceof IndexRowType) ||
                    (rowType instanceof HKeyRowType));
        }
        
        protected RowStream assembleUnion(SetPlanNode union) {
            PlanNode left = union.getLeft();
            if (left instanceof ResultSet)
                left = ((ResultSet) left).getInput();

            PlanNode right = union.getRight();
            if (right instanceof ResultSet)
                right = ((ResultSet) right).getInput();

            RowStream leftStream = assembleStream(left);
            RowStream rightStream = assembleStream(right);

            if (union.isAll()) {
                leftStream.operator =
                        API.unionAll_Default(leftStream.operator, leftStream.rowType,
                                rightStream.operator, rightStream.rowType,
                                rulesContext.getPipelineConfiguration().isUnionAllOpenBoth());
            } else {

                //Union ordered assumes sorted order, so sort the input streams.
                //TODO: Is there a way to determine if this is a requirement?
                leftStream.operator = API.sort_General(leftStream.operator, leftStream.rowType,
                        assembleSetOrdering(leftStream.rowType), API.SortOption.SUPPRESS_DUPLICATES);
                leftStream.rowType = leftStream.operator.rowType();

                rightStream.operator = API.sort_General(rightStream.operator, rightStream.rowType,
                        assembleSetOrdering(rightStream.rowType), API.SortOption.SUPPRESS_DUPLICATES);
                rightStream.rowType = rightStream.operator.rowType();

                boolean[] ascending = new boolean[rightStream.rowType.nFields()];
                Arrays.fill(ascending, Boolean.TRUE);
                RowType leftRowType = leftStream.rowType;
                RowType rightRowType = rightStream.rowType;
                int leftOrderingFields = leftRowType.nFields();
                int rightOrderingFields = rightRowType.nFields();
                leftStream.operator =
                        API.union_Ordered(leftStream.operator, rightStream.operator, leftRowType, rightRowType,
                                leftOrderingFields, rightOrderingFields, ascending, false);
            }
            leftStream.rowType = leftStream.operator.rowType();
            return leftStream;
        }

        protected RowStream assembleIntersect(SetPlanNode intersect) {
            PlanNode left = intersect.getLeft();
            if (left instanceof ResultSet)
                left = ((ResultSet)left).getInput();

            PlanNode right = intersect.getRight();
            if (right instanceof ResultSet)
                right = ((ResultSet)right).getInput();

            RowStream leftStream = assembleStream (left);
            RowStream rightStream = assembleStream (right);

            boolean[] ascending = new boolean[rightStream.rowType.nFields()];
            Arrays.fill(ascending, Boolean.TRUE);
            RowType leftRowType = leftStream.rowType;
            RowType rightRowType = rightStream.rowType;
            int leftOrderingFields = leftRowType.nFields();
            int rightOrderingFields = rightRowType.nFields();

            if (intersect.isAll()) {
                leftStream.operator = API.sort_General(leftStream.operator, leftStream.rowType,
                        assembleSetOrdering(leftStream.rowType), API.SortOption.PRESERVE_DUPLICATES);
                leftStream.rowType = leftStream.operator.rowType();

                rightStream.operator = API.sort_General(rightStream.operator, rightStream.rowType,
                        assembleSetOrdering(rightStream.rowType), API.SortOption.PRESERVE_DUPLICATES);
                rightStream.rowType = rightStream.operator.rowType();
                leftStream.operator =
                        API.intersect_Ordered(leftStream.operator,
                                                rightStream.operator,
                                                leftRowType,
                                                rightRowType,
                                                leftOrderingFields,
                                                rightOrderingFields,
                                                ascending,
                                                JoinType.INNER_JOIN,
                                                EnumSet.of(IntersectOption.OUTPUT_LEFT, IntersectOption.SEQUENTIAL_SCAN),
                                                null);
            } else {
                leftStream.operator = API.sort_General(leftStream.operator, leftStream.rowType,
                        assembleSetOrdering(leftStream.rowType), API.SortOption.SUPPRESS_DUPLICATES);
                leftStream.rowType = leftStream.operator.rowType();

                rightStream.operator = API.sort_General(rightStream.operator, rightStream.rowType,
                        assembleSetOrdering(rightStream.rowType), API.SortOption.SUPPRESS_DUPLICATES);
                rightStream.rowType = rightStream.operator.rowType();
                leftStream.operator = API.intersect_Ordered(leftStream.operator,
                                                rightStream.operator,
                                                leftRowType,
                                                rightRowType,
                                                leftOrderingFields,
                                                rightOrderingFields,
                                                ascending,
                                                JoinType.INNER_JOIN,
                                                EnumSet.of(IntersectOption.OUTPUT_LEFT, IntersectOption.SEQUENTIAL_SCAN),
                                                null);

            }//TODO add instance if input rows are already sorted
            leftStream.rowType = leftStream.operator.rowType();
            return leftStream;
        }

        protected RowStream assembleExcept(SetPlanNode except) {
            PlanNode left = except.getLeft();
            if (left instanceof ResultSet)
                left = ((ResultSet)left).getInput();

            PlanNode right = except.getRight();
            if (right instanceof ResultSet)
                right = ((ResultSet)right).getInput();

            RowStream leftStream = assembleStream (left);
            RowStream rightStream = assembleStream (right);

            boolean[] ascending = new boolean[rightStream.rowType.nFields()];
            Arrays.fill(ascending, Boolean.TRUE);
            RowType leftRowType = leftStream.rowType;
            RowType rightRowType = rightStream.rowType;
            int leftOrderingFields = leftRowType.nFields();
            int rightOrderingFields = rightRowType.nFields();

            if (except.isAll()) {
                leftStream.operator = API.sort_General(leftStream.operator, leftStream.rowType,
                        assembleSetOrdering(leftStream.rowType), API.SortOption.PRESERVE_DUPLICATES);
                leftStream.rowType = leftStream.operator.rowType();

                rightStream.operator = API.sort_General(rightStream.operator, rightStream.rowType,
                        assembleSetOrdering(rightStream.rowType), API.SortOption.PRESERVE_DUPLICATES);
                rightStream.rowType = rightStream.operator.rowType();
                leftStream.operator =
                        API.except_Ordered(leftStream.operator, rightStream.operator, leftRowType, rightRowType,
                                leftOrderingFields, rightOrderingFields, ascending, false);
            } else {
                leftStream.operator = API.sort_General(leftStream.operator, leftStream.rowType,
                        assembleSetOrdering(leftStream.rowType), API.SortOption.SUPPRESS_DUPLICATES);
                leftStream.rowType = leftStream.operator.rowType();

                rightStream.operator = API.sort_General(rightStream.operator, rightStream.rowType,
                        assembleSetOrdering(rightStream.rowType), API.SortOption.SUPPRESS_DUPLICATES);
                rightStream.rowType = rightStream.operator.rowType();
                leftStream.operator =
                        API.except_Ordered(leftStream.operator, rightStream.operator, leftRowType, rightRowType,
                                leftOrderingFields, rightOrderingFields, ascending, false);

            }//TODO add instance if input rows are already sorted
            leftStream.rowType = leftStream.operator.rowType();
            return leftStream;
        }

        protected API.Ordering assembleSetOrdering(RowType rowType) {
            API.Ordering ordering = createOrdering();
            for (int i = 0; i < rowType.nFields(); i++) {
                TPreparedExpression tExpr = field(rowType, i);

                if(rowType.fieldHasColumn(i))
                    ordering.append(tExpr, true, rowType.fieldColumn(i).getCollator());
                else
                    ordering.append(tExpr, true, AkCollatorFactory.UCS_BINARY_COLLATOR );
            }
            return ordering;
        }

        protected RowStream assembleMapJoin(MapJoin mapJoin) {
            PlanNode outer = mapJoin.getOuter();
            RowStream ostream = assembleStream(outer);
            int pos = pushBoundRow(ostream.fieldOffsets);
            nestedBindingsDepth++;
            RowStream stream = assembleStream(mapJoin.getInner());
            stream.operator = API.map_NestedLoops(ostream.operator, 
                                                  stream.operator,
                                                  pos,
                                                  rulesContext.getPipelineConfiguration().isMapEnabled(),
                                                  nestedBindingsDepth);
            nestedBindingsDepth--;
            popBoundRow();
            return stream;
        }

        protected RowStream assembleProduct(Product product) {
            TableRowType ancestorRowType = null;
            if (product.getAncestor() != null)
                ancestorRowType = tableRowType(product.getAncestor());
            RowStream pstream = new RowStream();
            Flattened flattened = new Flattened();
            pstream.fieldOffsets = flattened;
            int nbound = 0;
            int bindingPosition = -1;
            for (PlanNode subplan : product.getSubplans()) {
                if (pstream.operator != null) {
                    bindingPosition = pushBoundRow(flattened);
                    if (nbound++ == 0) {
                        // Only one deeper for all, since nest on the outer side.
                        nestedBindingsDepth++;
                    }
                }
                RowStream stream = assembleStream(subplan);
                if (pstream.operator == null) {
                    pstream.operator = stream.operator;
                    pstream.rowType = stream.rowType;
                }
                else {
                    stream.operator = API.product_Nested(stream.operator,
                                                         pstream.rowType,
                                                         ancestorRowType,
                                                         stream.rowType,
                                                         bindingPosition);
                    stream.rowType = stream.operator.rowType();
                    pstream.operator = API.map_NestedLoops(pstream.operator,
                                                           stream.operator,
                                                           bindingPosition,
                                                           rulesContext.getPipelineConfiguration().isMapEnabled(),
                                                           nestedBindingsDepth);
                    pstream.rowType = stream.rowType;
                }
                if (stream.fieldOffsets instanceof ColumnSourceFieldOffsets) {
                    TableSource table = ((ColumnSourceFieldOffsets)
                                         stream.fieldOffsets).getTable();
                    flattened.addTable(tableRowType(table), table);
                }
                else {
                    flattened.product((Flattened)stream.fieldOffsets);
                }
                flattened.setRowType(pstream.rowType);
            }
            if (nbound > 0) {
                nestedBindingsDepth--;
            }
            while (nbound > 0) {
                popBoundRow();
                nbound--;
            }
            return pstream;
        }

        protected RowStream assembleAggregateSource(AggregateSource aggregateSource) {
            AggregateSource.Implementation impl = aggregateSource.getImplementation();
            if (impl == null)
              impl = AggregateSource.Implementation.SORT;
            int nkeys = aggregateSource.getNGroupBy();
            RowStream stream;
            switch (impl) {
            case COUNT_STAR:
            case COUNT_TABLE_STATUS:
                {
                    assert !aggregateSource.isProjectSplitOff();
                    assert ((nkeys == 0) &&
                            (aggregateSource.getNAggregates() == 1));
                    if (impl == AggregateSource.Implementation.COUNT_STAR) {
                        stream = assembleStream(aggregateSource.getInput());
                        // TODO: Could be removed, since aggregate_Partial works as well.
                        stream.operator = API.count_Default(stream.operator, 
                                                            stream.rowType);
                    }
                    else {
                        stream = new RowStream();
                        stream.operator = API.count_TableStatus(tableRowType(aggregateSource.getTable()));
                    }
                    stream.rowType = stream.operator.rowType();
                    stream.fieldOffsets = new ColumnSourceFieldOffsets(aggregateSource, 
                                                                       stream.rowType);
                    return stream;
                }                
            }
            assert aggregateSource.isProjectSplitOff();
            stream = assembleStream(aggregateSource.getInput());
            switch (impl) {
            case PRESORTED:
            case UNGROUPED:
                break;
            case FIRST_FROM_INDEX:
                {
                    assert ((nkeys == 0) &&
                            (aggregateSource.getNAggregates() == 1));
                    stream.operator = API.limit_Default(stream.operator, 1);
                    stream = assembleNullIfEmpty(stream);
                    stream.fieldOffsets = new ColumnSourceFieldOffsets(aggregateSource, 
                                                                       stream.rowType);
                    return stream;
                }
            default:
                // TODO: Could pre-aggregate now in PREAGGREGATE_RESORT case.
                assembleSort(stream, nkeys, aggregateSource.getInput(),
                             API.SortOption.PRESERVE_DUPLICATES);
                break;
            }
            stream.operator = assembleAggregates(stream.operator, stream.rowType, nkeys,
                                                 aggregateSource);
            stream.rowType = stream.operator.rowType();
            stream.fieldOffsets = new ColumnSourceFieldOffsets(aggregateSource,
                                                               stream.rowType);
            return stream;
        }

        protected RowStream assembleDistinct(Distinct distinct) {
            Distinct.Implementation impl = distinct.getImplementation();
            if (impl == Distinct.Implementation.EXPLICIT_SORT) {
                PlanNode input = distinct.getInput();
                if (input instanceof Sort) {
                    // Explicit Sort is still there; combine.
                    return assembleSort((Sort)input, distinct.getOutput(), 
                                        API.SortOption.SUPPRESS_DUPLICATES);
                }
                // Sort removed but Distinct remains.
                impl = Distinct.Implementation.PRESORTED;
            }
            RowStream stream = assembleStream(distinct.getInput());
            if (impl == null)
              impl = Distinct.Implementation.SORT;
            switch (impl) {
            case PRESORTED:
                List<AkCollator> collators = findCollators(distinct.getInput());
                if (collators != null) {
                    stream.operator = API.distinct_Partial(stream.operator, stream.rowType, collators);
                } else {
                    throw new UnsupportedOperationException(String.format(
                        "Can't use Distinct_Partial except following a projection. Try again when types3 is in place"));
                }
                break;
            default:
                assembleSort(stream, stream.rowType.nFields(), distinct.getInput(),
                             API.SortOption.SUPPRESS_DUPLICATES);
                break;
            }
            return stream;
        }

        // Hack to handle some easy and common cases until types3.
        private List<AkCollator> findCollators(PlanNode node)
        {
            if (node instanceof Sort) {
                return findCollators(((Sort)node).getInput());
            } else if (node instanceof MapJoin) {
                return findCollators(((MapJoin)node).getInner());
            } else if (node instanceof Project) {
                List<AkCollator> collators = new ArrayList<>();
                Project project = (Project) node;
                for (ExpressionNode expressionNode : project.getFields()) {
                    collators.add(expressionNode.getCollator());
                }
                return collators;
            } else if (node instanceof IndexScan) {
                List<AkCollator> collators = new ArrayList<>();
                IndexScan indexScan = (IndexScan) node;
                for (IndexColumn indexColumn : indexScan.getIndexColumns()) {
                    collators.add(indexColumn.getColumn().getCollator());
                }
                return collators;
            } else {
                return null;
            }
        }

        protected RowStream assembleSort(Sort sort) {
            return assembleSort(sort, 
                                sort.getOutput(), API.SortOption.PRESERVE_DUPLICATES);
        }

        protected RowStream assembleSort(Sort sort, 
                                         PlanNode output, API.SortOption sortOption) {
            RowStream stream = assembleStream(sort.getInput());
            API.Ordering ordering = createOrdering();
            for (OrderByExpression orderBy : sort.getOrderBy()) {
                TPreparedExpression tExpr = assembleExpression(orderBy.getExpression(),
                        stream.fieldOffsets);
                ordering.append(tExpr, orderBy.isAscending(), orderBy.getCollator());
            }
            assembleSort(stream, ordering, sort.getInput(), output, sortOption);
            return stream;
        }

        protected void assembleSort(RowStream stream, API.Ordering ordering,
                                    PlanNode input, PlanNode output, 
                                    API.SortOption sortOption) {
            int maxrows = -1;
            if (output instanceof Project) {
                output = output.getOutput();
            }
            if (output instanceof Limit) {
                Limit limit = (Limit)output;
                if (!limit.isOffsetParameter() && !limit.isLimitParameter() &&
                    (limit.getLimit() >= 0)) {
                    maxrows = limit.getOffset() + limit.getLimit();
                }
            }
            else if (input instanceof ExpressionsSource) {
                ExpressionsSource expressionsSource = (ExpressionsSource)input;
                maxrows = expressionsSource.getExpressions().size();
            }
            if ((maxrows >= 0) && (maxrows <= INSERTION_SORT_MAX_LIMIT))
                stream.operator = API.sort_InsertionLimited(stream.operator, stream.rowType,
                                                            ordering, sortOption, maxrows);
            else
                stream.operator = API.sort_General(stream.operator, stream.rowType, ordering, sortOption);
        }

        protected void assembleSort(RowStream stream, int nkeys, PlanNode input,
                                    API.SortOption sortOption) {
            List<AkCollator> collators = findCollators(input);
            API.Ordering ordering = createOrdering();
            for (int i = 0; i < nkeys; i++) {
                TPreparedExpression tExpr = field(stream.rowType, i);
                ordering.append(tExpr, true,
                                (collators == null) ? null : collators.get(i));
            }
            assembleSort(stream, ordering, input, null, sortOption);
        }

        protected RowStream assembleBuffer(Buffer buffer) {
            RowStream stream = assembleStream(buffer.getInput());
            stream.operator = API.buffer_Default(stream.operator, stream.rowType);
            return stream;
        }

        protected RowStream assembleLimit(Limit limit) {
            RowStream stream = assembleStream(limit.getInput());
            int nlimit = limit.getLimit();
            if ((nlimit < 0) && !limit.isLimitParameter())
                nlimit = Integer.MAX_VALUE; // Slight disagreement in saying unlimited.
            stream.operator = API.limit_Default(stream.operator, 
                                                limit.getOffset(), limit.isOffsetParameter(),
                                                nlimit, limit.isLimitParameter());
            return stream;
        }

        protected RowStream assembleNullIfEmpty(NullIfEmpty nullIfEmpty) {
            RowStream stream = assembleStream(nullIfEmpty.getInput());
            return assembleNullIfEmpty(stream);
        }

        protected RowStream assembleNullIfEmpty(RowStream stream) {
            stream.operator = ifEmptyNulls(stream.operator, stream.rowType, API.InputPreservationOption.KEEP_INPUT);
            return stream;
        }

        protected RowStream assembleOnlyIfEmpty(OnlyIfEmpty onlyIfEmpty) {
            RowStream stream = assembleStream(onlyIfEmpty.getInput());
            stream.operator = API.limit_Default(stream.operator, 0, false, 1, false);
            // Nulls here have no semantic meaning, but they're easier than trying to
            // figure out an interesting non-null value for each
            // type in the row. All that really matters is that the
            // row is there.
            stream.operator = ifEmptyNulls(stream.operator, stream.rowType, API.InputPreservationOption.DISCARD_INPUT);
            return stream;
        }

        protected RowStream assembleUsingBloomFilter(UsingBloomFilter usingBloomFilter) {
            BloomFilter bloomFilter = usingBloomFilter.getBloomFilter();
            int pos = assignBindingPosition(bloomFilter);
            RowStream lstream = assembleStream(usingBloomFilter.getLoader());
            RowStream stream = assembleStream(usingBloomFilter.getInput());
            List<AkCollator> collators = null;
            if (usingBloomFilter.getLoader() instanceof IndexScan) {
                collators = new ArrayList<>();
                IndexScan indexScan = (IndexScan) usingBloomFilter.getLoader();
                for (IndexColumn indexColumn : indexScan.getIndexColumns()) {
                    collators.add(indexColumn.getColumn().getCollator());
                }
            }
            stream.operator = API.using_BloomFilter(lstream.operator,
                                                    lstream.rowType,
                                                    bloomFilter.getEstimatedSize(),
                                                    pos,
                                                    stream.operator,
                                                    collators);
            return stream;
        }

        protected RowStream assembleBloomFilterFilter(BloomFilterFilter bloomFilterFilter) {
            BloomFilter bloomFilter = bloomFilterFilter.getBloomFilter();
            int pos = getBindingPosition(bloomFilter);
            RowStream stream = assembleStream(bloomFilterFilter.getInput());
            bindingPositions.put(stream.fieldOffsets, pos); // Shares the slot.
            boundRows.push(stream.fieldOffsets);
            nestedBindingsDepth++;
            RowStream cstream = assembleStream(bloomFilterFilter.getCheck());
            boundRows.pop();
            List<TPreparedExpression> tFields = assembleExpressions(bloomFilterFilter.getLookupExpressions(),
                    stream.fieldOffsets);
            List<AkCollator> collators = new ArrayList<>();
            for (ExpressionNode expressionNode : bloomFilterFilter.getLookupExpressions()) {
                collators.add(expressionNode.getCollator());
            }
            stream.operator = API.select_BloomFilter(stream.operator,
                                                     cstream.operator,
                                                     tFields,
                                                     collators,
                                                     pos,
                                                     rulesContext.getPipelineConfiguration().isSelectBloomFilterEnabled(),
                                                     nestedBindingsDepth);
            nestedBindingsDepth--;
            return stream;
        }

        protected RowStream assembleUsingHashTable( UsingHashTable usingHashTable) {
            HashTable hashTable = usingHashTable.getHashTable();
            int pos = assignBindingPosition(hashTable);
            RowStream lstream = assembleStream(usingHashTable.getLoader());
            RowStream stream = assembleStream(usingHashTable.getInput());
            List<TPreparedExpression> tFields = assembleExpressions(usingHashTable.getLookupExpressions(),
                    lstream.fieldOffsets);
            List<AkCollator> collators = new ArrayList<>();
            RowType rt = lstream.rowType;
            for(int i = 0; i < rt.nFields(); i++){
                if(TInstance.tClass(rt.typeAt(i)) instanceof TString){
                    collators.add(TString.getCollator(rt.typeAt(i)));
                }
            }
            if(collators.isEmpty())
                collators = null;
            stream.operator = API.using_HashTable(lstream.operator,
                    lstream.rowType,
                    tFields,
                    pos,
                    stream.operator,
                    collators);
            return stream;
        }

        protected RowStream assembleHashTableLookup(HashTableLookup hashTableLookup) {
            HashTable hashTable = hashTableLookup.getHashTable();
            int tablePos = getBindingPosition(hashTable);
            RowStream stream = assembleStream(hashTableLookup.getInput());
            List<TPreparedExpression> tFields = assembleExpressions(hashTableLookup.getLookupExpressions(),
                    stream.fieldOffsets);
            List<AkCollator> collators = new ArrayList<>();
            for (ExpressionNode expressionNode : hashTableLookup.getLookupExpressions()) {
                collators.add(expressionNode.getCollator());
            }
            stream.operator = API.hashTableLookup_Default(
                    collators,
                    tFields,
                    tablePos);
            return stream;
        }

        protected RowStream assembleProject(Project project) {
            RowStream stream = assembleStream(project.getInput());
            List<? extends TPreparedExpression> pExpressions;
            pExpressions = assembleExpressions(project.getFields(), stream.fieldOffsets);
            stream.operator = API.project_Default(stream.operator,
                                                  stream.rowType,
                                                  pExpressions);
            stream.rowType = stream.operator.rowType();
            stream.fieldOffsets = new ColumnSourceFieldOffsets(project,
                                                               stream.rowType);
            return stream;
        }

        // Get a list of result columns based on ResultSet expression names.
        protected List<PhysicalResultColumn> getResultColumns(List<ResultField> fields) {
            List<PhysicalResultColumn> columns = 
                new ArrayList<>(fields.size());
            for (ResultField field : fields) {
                columns.add(rulesContext.getResultColumn(field));
            }
            return columns;
        }

        // Get a list of result columns for unnamed columns.
        // This would correspond to top-level VALUES, which the parser
        // does not currently support.
        protected List<PhysicalResultColumn> getResultColumns(int ncols) {
            List<PhysicalResultColumn> columns = 
                new ArrayList<>(ncols);
            for (int i = 0; i < ncols; i++) {
                columns.add(rulesContext.getResultColumn(new ResultField("column" + (i+1))));
            }
            return columns;
        }

        // Generate key range bounds.
        protected IndexKeyRange assembleIndexKeyRange(SingleIndexScan index, ColumnExpressionToIndex fieldOffsets) {
            return assembleIndexKeyRange(
                    index,
                    fieldOffsets,
                    index.getLowComparand(),
                    index.isLowInclusive(),
                    index.getHighComparand(),
                    index.isHighInclusive()
            );
        }

        protected IndexKeyRange assembleIndexKeyRange(SingleIndexScan index, ColumnExpressionToIndex fieldOffsets,
                                                      RangeSegment segment)
        {
            return assembleIndexKeyRange(
                    index,
                    fieldOffsets,
                    segment.getStart().getValueExpression(),
                    segment.getStart().isInclusive(),
                    segment.getEnd().getValueExpression(),
                    segment.getEnd().isInclusive()
            );
        }

        private IndexKeyRange assembleIndexKeyRange(SingleIndexScan index,
                                                    ColumnExpressionToIndex fieldOffsets,
                                                    ExpressionNode lowComparand,
                                                    boolean lowInclusive, ExpressionNode highComparand,
                                                    boolean highInclusive)
        {
            List<ExpressionNode> equalityComparands = index.getEqualityComparands();
            IndexRowType indexRowType = getIndexRowType(index);
            if ((equalityComparands == null) &&
                    (lowComparand == null) && (highComparand == null))
                return IndexKeyRange.unbounded(indexRowType);

            int nkeys = 0;
            if (equalityComparands != null)
                nkeys = equalityComparands.size();
            if ((lowComparand != null) || (highComparand != null))
                nkeys++;
            TPreparedExpression[] pkeys = new TPreparedExpression[nkeys];

            int kidx = 0;
            if (equalityComparands != null) {
                for (ExpressionNode comp : equalityComparands) {
                    if (!(comp instanceof IsNullIndexKey)) { // Java null means IS NULL; Null expression wouldn't match.
                        assembleExpressionInto(comp, fieldOffsets, pkeys, kidx);
                    }
                    kidx++;
                }
            }

            if ((lowComparand == null) && (highComparand == null)) {
                IndexBound eq = getIndexBound(index.getIndex(), pkeys, kidx);
                return IndexKeyRange.bounded(indexRowType, eq, true, eq, true);
            }
            else {
                TPreparedExpression[] lowPKeys = null, highPKeys = null;
                boolean lowInc = false, highInc = false;
                int lidx = kidx, hidx = kidx;
                if ((lidx > 0) || (lowComparand != null)) {
                    lowPKeys = pkeys;
                    if ((hidx > 0) || (highComparand != null)) {
                        highPKeys = new TPreparedExpression[nkeys];
                        System.arraycopy(pkeys, 0, highPKeys, 0, nkeys);
                    }
                }
                else if (highComparand != null) {
                    highPKeys = pkeys;
                }
                if (lowComparand != null) {
                    assembleExpressionInto(lowComparand, fieldOffsets, lowPKeys, lidx);
                    lidx++;
                    lowInc = lowInclusive;
                }
                if (highComparand != null) {
                    assembleExpressionInto(highComparand, fieldOffsets, highPKeys, hidx);
                    hidx++;
                    highInc = highInclusive;
                }
                int bounded = lidx > hidx ? lidx : hidx;
                IndexBound lo = getIndexBound(index.getIndex(), lowPKeys, bounded);
                IndexBound hi = getIndexBound(index.getIndex(), highPKeys, bounded);
                assert lo != null || hi != null;
                if (lo == null) {
                    lo = getNullIndexBound(index.getIndex(), hidx);
                }
                if (hi == null) {
                    hi = getNullIndexBound(index.getIndex(), lidx);
                }
                return IndexKeyRange.bounded(indexRowType, lo, lowInc, hi, highInc);
            }
        }

        protected API.Ordering assembleIndexOrdering(IndexScan index,
                                                     IndexRowType indexRowType) {
            API.Ordering ordering = createOrdering();
            List<OrderByExpression> indexOrdering = index.getOrdering();
            for (int i = 0; i < indexOrdering.size(); i++) {
                TPreparedExpression tExpr = field(indexRowType, i);
                ordering.append(tExpr,
                                indexOrdering.get(i).isAscending(),
                                index.getIndexColumns().get(i).getColumn().getCollator());
            }
            return ordering;
        }

        protected TableRowType tableRowType(TableSource table) {
            return tableRowType(table.getTable());
        }

        protected TableRowType tableRowType(TableNode table) {
            Table aisTable = table.getTable();
            affectedTables.add(aisTable);
            return schema.tableRowType(aisTable);
        }

        protected IndexRowType getIndexRowType(SingleIndexScan index) {
            Index aisIndex = index.getIndex();
            AkibanInformationSchema ais = schema.ais();
            for (int i : aisIndex.getAllTableIDs()) {
                affectedTables.add(ais.getTable(i));
            }
            return schema.indexRowType(aisIndex);
        }

        /** Return an index bound for the given index and expressions.
         * @param index the index in use
         * @param keys {@link Expression}s for index lookup key
         * @param nBoundKeys number of keys actually in use
         */
        protected IndexBound getIndexBound(Index index, TPreparedExpression[] pKeys,
                                           int nBoundKeys) {
            if (pKeys == null)
                return null;
            TPreparedExpression[] boundPKeys;
            int nkeys = pKeys.length;
            if (nBoundKeys < nkeys) {
                Object[] source, dest;
                boundPKeys = new TPreparedExpression[nBoundKeys];
                source = pKeys;
                dest = boundPKeys;
                System.arraycopy(source, 0, dest, 0, nBoundKeys);
            } else {
                boundPKeys = pKeys;
            }
            fillNulls(index, pKeys);
            return new IndexBound(getIndexExpressionRow(index, boundPKeys),
                                  getIndexColumnSelector(index, nBoundKeys));
        }

        /** Return an index bound for the given index containing all nulls.
         * @param index the index in use
         * @param nkeys number of keys actually in use
         */
        protected IndexBound getNullIndexBound(Index index, int nkeys) {
            TPreparedExpression[] pKeys = createNulls(index, nkeys);
            return new IndexBound(getIndexExpressionRow(index, pKeys),
                                  getIndexColumnSelector(index, nkeys));
        }

        protected IndexKeyRange assembleSpatialIndexKeyRange(SingleIndexScan index, ColumnExpressionToIndex fieldOffsets) {
            FunctionExpression func = (FunctionExpression)index.getLowComparand();
            List<ExpressionNode> operands = func.getOperands();
            IndexRowType indexRowType = getIndexRowType(index);
            if ("_center".equals(func.getFunction())) {
                return IndexKeyRange.spatial(indexRowType,
                                             assembleSpatialIndexPoint(index,
                                                                       operands.get(0),
                                                                       operands.get(1),
                                                                       fieldOffsets),
                                             null);
            }
            else if ("_center_radius".equals(func.getFunction())) {
                ExpressionNode centerY = operands.get(0);
                ExpressionNode centerX = operands.get(1);
                ExpressionNode radius = operands.get(2);
                // Make circle into box. Comparison still remains to eliminate corners.
                // TODO: May need some casts.
                ExpressionNode bottom = new FunctionExpression("minus",
                                                               Arrays.asList(centerY, radius),
                                                               null, null, null);
                ExpressionNode left = new FunctionExpression("minus",
                                                             Arrays.asList(centerX, radius),
                                                             null, null, null);
                ExpressionNode top = new FunctionExpression("plus",
                                                            Arrays.asList(centerY, radius),
                                                            null, null, null);
                ExpressionNode right = new FunctionExpression("plus",
                                                              Arrays.asList(centerX, radius),
                                                              null, null, null);
                bottom = resolveAddedExpression(bottom, planContext);
                left = resolveAddedExpression(left, planContext);
                top = resolveAddedExpression(top, planContext);
                right = resolveAddedExpression(right, planContext);
                return IndexKeyRange.spatial(indexRowType,
                                             assembleSpatialIndexPoint(index, bottom, left, fieldOffsets),
                                             assembleSpatialIndexPoint(index, top, right, fieldOffsets));
            }
            else {
                throw new AkibanInternalException("Unrecognized spatial index " + index);
            }
        }

        protected IndexBound assembleSpatialIndexPoint(SingleIndexScan index, ExpressionNode y, ExpressionNode x, ColumnExpressionToIndex fieldOffsets) {
            List<ExpressionNode> equalityComparands = index.getEqualityComparands();
            int nkeys = 0;
            if (equalityComparands != null)
                nkeys = equalityComparands.size();
            nkeys += 2;
            TPreparedExpression[] pkeys = new TPreparedExpression[nkeys];
            int kidx = 0;
            if (equalityComparands != null) {
                for (ExpressionNode comp : equalityComparands) {
                    if (comp != null) {
                        assembleExpressionInto(comp, fieldOffsets, pkeys, kidx);
                    }
                    kidx++;
                }
            }
            assembleExpressionInto(y, fieldOffsets, pkeys, kidx++);
            assembleExpressionInto(x, fieldOffsets, pkeys, kidx++);
            assert (kidx == nkeys) : "kidx (" +kidx + ") != nkeys (" + nkeys + ")";
            return getIndexBound(index.getIndex(), pkeys, nkeys);
        }

        /** Return a column selector that enables the first <code>nkeys</code> fields
         * of a row of the index's user table. */
        protected ColumnSelector getIndexColumnSelector(final Index index, 
                                                        final int nkeys) {
            assert nkeys <= index.getAllColumns().size() : index + " " + nkeys;
            return new IndexRowPrefixSelector(nkeys);
        }

        /** Return a {@link Row} for the given index containing the given
         * {@link Expression} values.  
         */
        protected UnboundExpressions getIndexExpressionRow(Index index, 
                                                           TPreparedExpression[] pKeys) {
            RowType rowType = schema.indexRowType(index);
            List<TPreparedExpression> pExprs = Arrays.asList(pKeys);
            return new RowBasedUnboundExpressions(rowType, pExprs);
        }

        // Get the required type for any parameters to the statement.
        protected BasePlannable.ParameterType[] getParameterTypes() {
            AST ast = ASTStatementLoader.getAST(planContext);
            if (ast == null)
                return null;
            List<ParameterNode> params = ast.getParameters();
            if ((params == null) || params.isEmpty())
                return null;
            int nparams = 0;
            for (ParameterNode param : params) {
                if (nparams < param.getParameterNumber() + 1)
                    nparams = param.getParameterNumber() + 1;
            }
            TypesTranslator typesTranslator = rulesContext.getTypesTranslator();
            BasePlannable.ParameterType[] result = new BasePlannable.ParameterType[nparams];
            for (ParameterNode param : params) {
                int paramNo = param.getParameterNumber();
                if (result[paramNo] == null) {
                    DataTypeDescriptor sqlType = param.getType();
                    TInstance type = (TInstance)param.getUserData();
                    if (type == null)
                        assert param.isReturnOutputParam() : param;
                    else
                        result[paramNo] = new BasePlannable.ParameterType(sqlType, type);
                }
            }
            return result;
        }

        protected RowStream assembleFullTextScan(FullTextScan textScan) {
            RowStream stream = new RowStream();
            FullTextQueryBuilder builder = new FullTextQueryBuilder(textScan.getIndex(),
                                                                    schema.ais(),
                                                                    planContext.getQueryContext());
            FullTextQueryExpression queryExpression = assembleFullTextQuery(textScan.getQuery(),
                                                                            builder);
            stream.operator = builder.scanOperator(queryExpression, textScan.getLimit());
            stream.rowType = stream.operator.rowType();
            return stream;
        }

        protected FullTextQueryExpression assembleFullTextQuery(FullTextQuery query,
                                                                FullTextQueryBuilder builder) {
            if (query instanceof FullTextField) {
                FullTextField field = (FullTextField)query;
                ExpressionNode key = field.getKey();
                TPreparedExpression expr = null;
                String constant = null;
                boolean isConstant = false;
                if (key.isConstant()) {
                    ValueSource valueSource = key.getPreptimeValue().value();
                    constant = (valueSource == null || valueSource.isNull()) ? null : valueSource.getString();
                    isConstant = true;
                }
                else {
                    expr = assembleExpression(key, null);
                }
                switch (field.getType()) {
                case PARSE:
                    if (isConstant)
                        return builder.parseQuery(field.getIndexColumn(), constant);
                    else
                        return builder.parseQuery(field.getIndexColumn(), expr);
                case MATCH:
                    if (isConstant)
                        return builder.matchQuery(field.getIndexColumn(), constant);
                    else
                        return builder.matchQuery(field.getIndexColumn(), expr);
                }
            }
            else if (query instanceof FullTextBoolean) {
                FullTextBoolean bquery = (FullTextBoolean)query;
                List<FullTextQuery> operands = bquery.getOperands();
                List<FullTextQueryExpression> queries = new ArrayList<>(operands.size());
                for (FullTextQuery operand : operands) {
                    queries.add(assembleFullTextQuery(operand, builder));
                }
                return builder.booleanQuery(queries, bquery.getTypes());
            }
            throw new UnsupportedSQLException("Full text query " + query, null);
        }

        /* Expressions-related */

        // Assemble a list of expressions from the given nodes.
        public List<TPreparedExpression> assembleExpressions(List<ExpressionNode> expressions,
                                           ColumnExpressionToIndex fieldOffsets) {
            List<TPreparedExpression> result = new ArrayList<>(expressions.size());
            for (ExpressionNode expr : expressions) {
                result.add(assembleExpression(expr, fieldOffsets));
            }
            return result;
        }

        public void assembleExpressionInto(ExpressionNode expr, ColumnExpressionToIndex fieldOffsets, TPreparedExpression[] arr,
                                           int i) {
            TPreparedExpression result = assembleExpression(expr, fieldOffsets);
            arr[i] = result;
        }

        // Assemble a list of expressions from the given nodes.
        public List<TPreparedExpression> assembleExpressionsA(List<? extends AnnotatedExpression> expressions,
                                            ColumnExpressionToIndex fieldOffsets) {
            List<TPreparedExpression> result = new ArrayList<>(expressions.size());
            for (AnnotatedExpression aexpr : expressions) {
                result.add(assembleExpression(aexpr.getExpression(), fieldOffsets));
            }
            return result;
        }

            // Assemble an expression against the given row offsets.
        public TPreparedExpression assembleExpression(ExpressionNode expr, ColumnExpressionToIndex fieldOffsets) {
            ColumnExpressionContext context = getColumnExpressionContext(fieldOffsets);
            return expressionAssembler.assembleExpression(expr, context, this);
        }

        // Assemble an aggregate operator
        public Operator assembleAggregates(Operator inputOperator, RowType inputRowType, int inputsIndex,
                                           AggregateSource aggregateSource) {
            return expressionAssembler.assembleAggregates(inputOperator, inputRowType, inputsIndex, aggregateSource);
        }

        protected List<? extends TPreparedExpression> createNulls(RowType rowType) {
            int nfields = rowType.nFields();
            List<TPreparedExpression> result = new ArrayList<>(nfields);
            for (int i = 0; i < nfields; ++i)
                result.add(nullExpression(rowType, i));
            return result;
        }

        @Override
        public TPreparedExpression assembleSubqueryExpression(SubqueryExpression sexpr) {
            ColumnExpressionToIndex fieldOffsets = columnBoundRows.current;
            RowType outerRowType = null;
            if (fieldOffsets != null)
                outerRowType = fieldOffsets.getRowType();
            int pos = pushBoundRow(fieldOffsets);
            PlanNode subquery = sexpr.getSubquery().getQuery();
            ExpressionNode expression = null;
            boolean fieldExpression = false;
            if ((sexpr instanceof AnyCondition) ||
                (sexpr instanceof SubqueryValueExpression)) {
                if (subquery instanceof ResultSet)
                    subquery = ((ResultSet)subquery).getInput();
                if (subquery instanceof Project) {
                    Project project = (Project)subquery;
                    subquery = project.getInput();
                    expression = project.getFields().get(0);
                }
                else {
                    fieldExpression = true;
                }
            }
            RowStream stream = assembleQuery(subquery);
            TPreparedExpression innerExpression = null;
            if (fieldExpression)
                innerExpression = field(stream.rowType, 0);
            else if (expression != null)
                innerExpression = assembleExpression(expression, stream.fieldOffsets);
            TPreparedExpression result = assembleSubqueryExpression(sexpr,
                                                  stream.operator,
                                                  innerExpression,
                                                  outerRowType,
                                                  stream.rowType,
                                                  pos);
            popBoundRow();
            columnBoundRows.current = fieldOffsets;
            return result;
        }

        private TPreparedExpression assembleSubqueryExpression(SubqueryExpression sexpr,
                                             Operator operator,
                                             TPreparedExpression innerExpression,
                                             RowType outerRowType,
                                             RowType innerRowType,
                                             int bindingPosition) {
            if (sexpr instanceof ExistsCondition)
                return existsExpression(operator, outerRowType,
                                        innerRowType, bindingPosition);
            else if (sexpr instanceof AnyCondition)
                return anyExpression(operator, innerExpression,
                                     outerRowType, innerRowType, bindingPosition);
            else if (sexpr instanceof SubqueryValueExpression)
                return scalarSubqueryExpression(operator, innerExpression,
                                                outerRowType, innerRowType,
                                                bindingPosition);
            else if (sexpr instanceof SubqueryResultSetExpression)
                return resultSetSubqueryExpression(operator, sexpr.getPreptimeValue(),
                                                   outerRowType, innerRowType, 
                                                   bindingPosition);
            else
                throw new UnsupportedSQLException("Unknown subquery", sexpr.getSQLsource());
        }

        public List<TPreparedExpression> assembleUpdates(TableRowType targetRowType, List<UpdateColumn> updateColumns,
                                           ColumnExpressionToIndex fieldOffsets) {
            List<TPreparedExpression> updates = assembleExpressionsA(updateColumns, fieldOffsets);
            // Have a list of expressions in the order specified.
            // Want a list as wide as the target row with Java nulls
            // for the gaps.
            // TODO: It might be simpler to have an update function
            // that knew about column offsets for ordered expressions.
            TPreparedExpression[] row = array(targetRowType.nFields());
            for (int i = 0; i < updateColumns.size(); i++) {
                UpdateColumn column = updateColumns.get(i);
                row[column.getColumn().getPosition()] = updates.get(i);
            }
            updates = Arrays.asList(row);
            return updates;
        }

        public TPreparedExpression[] createNulls(Index index, int nkeys) {
            TPreparedExpression[] arr = array(nkeys);
            fillNulls(index, arr);
            return arr;
        }
 
        public void fillNulls(Index index, TPreparedExpression[] keys) {
            List<IndexColumn> indexColumns = index.getAllColumns();
            for (int i = 0; i < keys.length; ++i) {
                if (keys[i] == null)
                    keys[i] = new TNullExpression(indexColumns.get(i).getColumn().getType());
            }
        }

        public RowType valuesRowType(ExpressionsSource expressionsSource) {
            TInstance[] types = expressionsSource.getFieldTInstances();
            return schema.newValuesType(types);
        }

        protected TPreparedExpression existsExpression(Operator operator, RowType outerRowType,
                                                       RowType innerRowType,
                                                       int bindingPosition) {
            return new ExistsSubqueryTExpression(operator, outerRowType, innerRowType, bindingPosition);
        }

        protected TPreparedExpression anyExpression(Operator operator, TPreparedExpression innerExpression,
                                                    RowType outerRowType,
                                                    RowType innerRowType, int bindingPosition) {
            return new AnySubqueryTExpression(operator, innerExpression, outerRowType, innerRowType, bindingPosition);
        }
        
        protected TPreparedExpression scalarSubqueryExpression(Operator operator, TPreparedExpression innerExpression,
                                                               RowType outerRowType, RowType innerRowType,
                                                               int bindingPosition) {
            return new ScalarSubqueryTExpression(operator, innerExpression, outerRowType, innerRowType, bindingPosition);
        }

        protected TPreparedExpression resultSetSubqueryExpression(Operator operator, TPreptimeValue preptimeValue,
                                                                  RowType outerRowType, RowType innerRowType, int bindingPosition) {
            return new ResultSetSubqueryTExpression(operator, preptimeValue.type(), outerRowType, innerRowType, bindingPosition);
        }

        public TPreparedExpression field(RowType rowType, int position) {
            return new TPreparedField(rowType.typeAt(position), position);
        }

        protected TPreparedExpression[] array(int size) {
            return new TPreparedExpression[size];
        }

        public Operator ifEmptyNulls(Operator input, RowType rowType,
                                     InputPreservationOption inputPreservation) {
            return API.ifEmpty_Default(input, rowType, createNulls(rowType), inputPreservation);
        }

        protected TPreparedExpression nullExpression(RowType rowType, int i) {
            return new TNullExpression(rowType.typeAt(i));
        }

        public API.Ordering createOrdering() {
            return API.ordering();
        }

        public ExpressionNode resolveAddedExpression(ExpressionNode expr,
                                                     PlanContext planContext) {
            ExpressionRewriteVisitor visitor = TypeResolver.getResolver(planContext);
            return expr.accept(visitor);
        }

        /* Bindings-related state */

        protected int nestedBindingsDepth = 0;
        // boundRows is the dynamic list available.
        protected Deque<ColumnExpressionToIndex> boundRows = new ArrayDeque<>();
        // bindings is complete list of assignments; bindingPositions its inverse.
        protected List<Object> bindings = new ArrayList<>();
        protected Map<Object,Integer> bindingPositions = new HashMap<>();

        protected int assignBindingPosition(Object binding) {
            int position = bindings.size();
            bindings.add(binding);
            bindingPositions.put(binding, position);
            return position;
        }

        protected int getBindingPosition(Object binding) {
            return bindingPositions.get(binding);
        }

        protected void initializeBindings() {
            // Binding positions start with parameter positions.

            AST ast = ASTStatementLoader.getAST(planContext);
            if (ast != null) {
                List<ParameterNode> params = ast.getParameters();
                if (params != null) {
                    for (ParameterNode param : params) {
                        assignBindingPosition(param);
                    }
                }
            }
        }

        protected static final class NullBoundRow implements ColumnExpressionToIndex {
            @Override
            public int getIndex(ColumnExpression column) {
                return -1;
            }

            @Override
            public RowType getRowType() {
                return null;
            }

            public String toString() {
                return "null";
            }
        }

        protected int pushBoundRow(ColumnExpressionToIndex boundRow) {
            if (boundRow == null)
                boundRow = new NullBoundRow();
            boundRows.push(boundRow);
            return assignBindingPosition(boundRow);
        }

        protected void popBoundRow() {
            boundRows.pop();
        }

        class ColumnBoundRows implements ColumnExpressionContext {
            ColumnExpressionToIndex current;

            @Override
            public ColumnExpressionToIndex getCurrentRow() {
                return current;
            }

            @Override
            public Iterable<ColumnExpressionToIndex> getBoundRows() {
                return boundRows;
            }

            @Override
            public int getBindingPosition(ColumnExpressionToIndex boundRow) {
                return Assembler.this.getBindingPosition(boundRow);
            }

            @Override
            public RowType getRowType(CreateAs createAs){
                return Assembler.this.tableRowType(createAs.getTableSource());
            }

            @Override
            public RowType getRowType(int tableID){
                return schema.tableRowType(tableID);
            }
        }
        
        ColumnBoundRows columnBoundRows = new ColumnBoundRows();

        protected ColumnExpressionContext getColumnExpressionContext(ColumnExpressionToIndex current) {
            columnBoundRows.current = current;
            return columnBoundRows;
        }

        protected ColumnExpressionToIndex lookupNestedBoundRow(GroupLoopScan scan) {
            // Find the outside key's binding position.
            ColumnExpression joinColumn = scan.getOutsideJoinColumn();
            for (ColumnExpressionToIndex boundRow : boundRows) {
                int fieldIndex = boundRow.getIndex(joinColumn);
                if (fieldIndex >= 0) return boundRow;
            }
            throw new AkibanInternalException("Outer loop not found " + scan);
        }

        protected ColumnExpressionToIndex lookupNestedBoundRow(TableSource table) {
            // Find the target table's binding position.
            for (ColumnExpressionToIndex boundRow : boundRows) {
                if (boundRowIsForTable(boundRow, table)) {
                    return boundRow;
                }
            }
            throw new AkibanInternalException("Outer loop not found " + table);
        }

        protected boolean boundRowIsForTable(ColumnExpressionToIndex boundRow,
                                             TableSource table) {
            return ((boundRow instanceof ColumnSourceFieldOffsets) &&
                    (((ColumnSourceFieldOffsets)boundRow).getSource()) == table);
        }
    }

    // Struct for multiple value return from assembly.
    static class RowStream {
        Operator operator;
        RowType rowType;
        boolean unknownTypesPresent;
        ColumnExpressionToIndex fieldOffsets;
    }

    static abstract class BaseColumnExpressionToIndex implements ColumnExpressionToIndex {
        protected RowType rowType;

        BaseColumnExpressionToIndex(RowType rowType) {
            this.rowType = rowType;
        }

        @Override
        public RowType getRowType() {
            return rowType;
        }
    }

    // Single table-like source.
    static class ColumnSourceFieldOffsets extends BaseColumnExpressionToIndex {
        private ColumnSource source;

        public ColumnSourceFieldOffsets(ColumnSource source, RowType rowType) {
            super(rowType);
            this.source = source;
        }

        public ColumnSource getSource() {
            return source;
        }

        public TableSource getTable() {
            return (TableSource)source;
        }

        @Override
        public int getIndex(ColumnExpression column) {
            if (column.getTable() == source) 
                return column.getPosition();
            else if (source instanceof Project)
                return ((Project)source).getFields().indexOf(column);
            else
                return -1;
        }

        @Override
        public String toString() {
            return super.toString() + "(" + source + ")";
        }
    }

    // Index used as field source (e.g., covering).
    static class IndexFieldOffsets extends BaseColumnExpressionToIndex {
        private IndexScan index;

        public IndexFieldOffsets(IndexScan index, RowType rowType) {
            super(rowType);
            this.index = index;
        }

        @Override
        // Access field of the index row itself. 
        // (Covering index or condition before lookup.)
        public int getIndex(ColumnExpression column) {
            return index.getColumns().indexOf(column);
        }


        @Override
        public String toString() {
            return super.toString() + "(" + index + ")";
        }
    }

    // Flattened row.
    static class Flattened extends BaseColumnExpressionToIndex {
        Map<TableSource,Integer> tableOffsets = new HashMap<>();
        int nfields;
            
        Flattened() {
            super(null);        // Worked out later.
        }

        public void setRowType(RowType rowType) {
            this.rowType = rowType;
        }

        @Override
        public int getIndex(ColumnExpression column) {
            Integer tableOffset = tableOffsets.get(column.getTable());
            if (tableOffset == null)
                return -1;
            return tableOffset + column.getPosition();
        }

        public void addTable(RowType rowType, TableSource table) {
            if (table != null)
                tableOffsets.put(table, nfields);
            nfields += rowType.nFields();
        }

        // Tack on another flattened using product rules.
        public void product(final Flattened other) {
            List<TableSource> otherTables = 
                new ArrayList<>(other.tableOffsets.keySet());
            Collections.sort(otherTables,
                             new Comparator<TableSource>() {
                                 @Override
                                 public int compare(TableSource x, TableSource y) {
                                     return other.tableOffsets.get(x) - other.tableOffsets.get(y);
                                 }
                             });
            for (int i = 0; i < otherTables.size(); i++) {
                TableSource otherTable = otherTables.get(i);
                if (!tableOffsets.containsKey(otherTable)) {
                    tableOffsets.put(otherTable, nfields);
                    // Width in other.tableOffsets.
                    nfields += (((i+1 >= otherTables.size()) ?
                                 other.nfields :
                                 other.tableOffsets.get(otherTables.get(i+1))) -
                                other.tableOffsets.get(otherTable));
                }
            }
        }


        @Override
        public String toString() {
            return super.toString() + "(" + tableOffsets.keySet() + ")";
        }
    }
}
