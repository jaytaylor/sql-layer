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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.exec.UpdatePlannable;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TComparison;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;

import java.util.*;

public class API
{
    // Aggregate

    public static Operator aggregate_Partial(Operator inputOperator,
                                             RowType rowType,
                                             int inputsIndex,
                                             List<? extends TAggregator> aggregatorFactories,
                                             List<? extends TInstance> aggregatorTypes,
                                             List<Object> options
                                             )
    {
        return new Aggregate_Partial(inputOperator, rowType, inputsIndex, aggregatorFactories, aggregatorTypes, options);
    }

    // Project

    public static Operator project_DefaultTest(Operator inputOperator,
                                           RowType rowType,
                                           List<ExpressionGenerator> projections)
    {
        return new Project_Default(inputOperator, rowType, generateNew(projections));
    }

    public static List<TPreparedExpression> generateNew(List<? extends ExpressionGenerator> expressionGenerators) {
        if (expressionGenerators == null)
            return null;
        List<TPreparedExpression> results = new ArrayList<>(expressionGenerators.size());
        for (ExpressionGenerator generator : expressionGenerators) {
            results.add(generator.getTPreparedExpression());
        }
        return results;
    }

    public static Operator project_Default(Operator inputOperator,
                                                   RowType rowType,
                                                   List<? extends TPreparedExpression> pExpressions)
    {
        return new Project_Default(inputOperator, rowType, pExpressions);
    }
    
    public static Operator project_Default(Operator inputOperator, 
                                            List<ExpressionGenerator> expressionGenerators,
                                            RowType rowType) 
    {
        return new Project_Default(inputOperator, rowType, generateNew(expressionGenerators));
    }
    
    public static Operator project_Table(Operator inputOperator,
                                                 RowType inputRowType,
                                                 RowType outputRowType,
                                                 List<? extends TPreparedExpression> pExpressions)
    {
        return new Project_Default(inputOperator, inputRowType, outputRowType, pExpressions);
    }
    // Flatten

    public static Operator flatten_HKeyOrdered(Operator inputOperator,
                                                       RowType parentType,
                                                       RowType childType,
                                                       JoinType joinType)
    {
        return flatten_HKeyOrdered(inputOperator, parentType, childType, joinType, EnumSet.noneOf(FlattenOption.class));
    }

    public static Operator flatten_HKeyOrdered(Operator inputOperator,
                                                       RowType parentType,
                                                       RowType childType,
                                                       JoinType joinType,
                                                       FlattenOption flag0)
    {
        return new Flatten_HKeyOrdered(inputOperator, parentType, childType, joinType, EnumSet.of(flag0));
    }

    public static Operator flatten_HKeyOrdered(Operator inputOperator,
                                                       RowType parentType,
                                                       RowType childType,
                                                       JoinType joinType,
                                                       FlattenOption flag0,
                                                       FlattenOption flag1)
    {
        return new Flatten_HKeyOrdered(inputOperator, parentType, childType, joinType, EnumSet.of(flag0, flag1));
    }

    public static Operator flatten_HKeyOrdered(Operator inputOperator,
                                                       RowType parentType,
                                                       RowType childType,
                                                       JoinType joinType,
                                                       EnumSet<FlattenOption> flags)
    {
        return new Flatten_HKeyOrdered(inputOperator, parentType, childType, joinType, flags);
    }

    // GroupScan

    public static Operator groupScan_Default(Group group)
    {
        return new GroupScan_Default(new GroupScan_Default.FullGroupCursorCreator(group));
    }

    public static Operator groupScan_Default(Group group,
                                             int hKeyBindingPosition,
                                             boolean deep,
                                             Table hKeyType,
                                             Table shortenUntil)
    {
        return new GroupScan_Default(
                new GroupScan_Default.PositionalGroupCursorCreator(group, hKeyBindingPosition, deep, hKeyType, shortenUntil));
    }

    // ValuesScan

    public static Operator valuesScan_Default (Collection<? extends BindableRow> rows, RowType rowType)
    {
        return new ValuesScan_Default (rows, rowType);
    }
    
    // BranchLookup

    public static Operator branchLookup_Default(Operator inputOperator,
                                                Group group,
                                                RowType inputRowType,
                                                TableRowType outputRowType,
                                                InputPreservationOption flag)
    {
        return groupLookup_Default(inputOperator, group, inputRowType, branchOutputRowTypes(outputRowType), flag, 1);
    }

    protected static List<TableRowType> branchOutputRowTypes(TableRowType outputRowType) {
        List<TableRowType> outputRowTypes = new ArrayList<>();
        outputRowTypes.add(outputRowType);
        Schema schema = (Schema)outputRowType.schema();
        for (RowType rowType : Schema.descendentTypes(outputRowType, schema.userTableTypes())) {
            outputRowTypes.add((TableRowType)rowType);
        }
        return outputRowTypes;
    }

    /** deprecated */
    public static Operator branchLookup_Nested(Group group,
                                               RowType inputRowType,
                                               TableRowType outputRowType,
                                               InputPreservationOption flag,
                                               int inputBindingPosition)
    {
        return branchLookup_Nested(group,
                                   inputRowType, 
                                   inputRowType,
                                   null,
                                   branchOutputRowTypes(outputRowType),
                                   flag,
                                   inputBindingPosition,
                                   1);
    }

    public static Operator branchLookup_Nested(Group group,
                                               RowType inputRowType,
                                               TableRowType ancestorRowType,
                                               TableRowType outputRowType,
                                               InputPreservationOption flag,
                                               int inputBindingPosition)
    {
        return branchLookup_Nested(group,
                                   inputRowType, 
                                   inputRowType,
                                   ancestorRowType,
                                   branchOutputRowTypes(outputRowType),
                                   flag,
                                   inputBindingPosition,
                                   1);
    }

    public static Operator branchLookup_Nested(Group group,
                                               RowType inputRowType,
                                               RowType sourceRowType,
                                               TableRowType ancestorRowType,
                                               Collection<TableRowType> outputRowTypes,
                                               InputPreservationOption flag,
                                               int inputBindingPosition,
                                               int lookaheadQuantum)
    {
        return new BranchLookup_Nested(group,
                                       inputRowType, 
                                       sourceRowType,
                                       ancestorRowType,
                                       outputRowTypes,
                                       flag,
                                       inputBindingPosition,
                                       lookaheadQuantum);
    }

    // Limit

    public static Operator limit_Default(Operator inputOperator, int limitRows)
    {
        return new Limit_Default(inputOperator, limitRows);
    }

    public static Operator limit_Default(Operator inputOperator,
                                                 int skipRows,
                                                 boolean skipIsBinding,
                                                 int limitRows,
                                                 boolean limitIsBinding)
    {
        return new Limit_Default(inputOperator, skipRows, skipIsBinding, limitRows, limitIsBinding);
    }

    // AncestorLookup

    public static Operator ancestorLookup_Default(Operator inputOperator,
                                                  Group group,
                                                  RowType rowType,
                                                  Collection<TableRowType> ancestorTypes,
                                                  InputPreservationOption flag)
    {
        return groupLookup_Default(inputOperator, group, rowType, ancestorTypes, flag, 1);
    }

    public static Operator groupLookup_Default(Operator inputOperator,
                                               Group group,
                                               RowType rowType,
                                               Collection<TableRowType> ancestorTypes,
                                               InputPreservationOption flag,
                                               int lookaheadQuantum)
    {
        return new GroupLookup_Default(inputOperator, group, rowType, ancestorTypes, flag, lookaheadQuantum);
    }

    public static Operator ancestorLookup_Nested(Group group,
                                                 RowType rowType,
                                                 Collection<TableRowType> ancestorTypes,
                                                 int hKeyBindingPosition,
                                                 int lookaheadQuantum)
    {
        return new AncestorLookup_Nested(group, rowType, ancestorTypes, hKeyBindingPosition, lookaheadQuantum);
    }

    // IndexScan

    /**
     * Creates a full ascending scan operator for the given index using LEFT JOIN semantics after the indexType's
     * tableType
     * @param indexType the index to scan
     * @return the scan operator
     * @deprecated use {@link #indexScan_Default(IndexRowType, IndexKeyRange, Ordering, IndexScanSelector)}
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static Operator indexScan_Default(IndexRowType indexType)
    {
        return indexScan_Default(indexType, false, IndexKeyRange.unbounded(indexType));
    }

    /**
     * Creates a full ascending scan operator for the given index using LEFT JOIN semantics after the indexType's
     * tableType
     * @param indexType the index to scan
     * @param reverse whether to scan in reverse order
     * @return the scan operator
     * @deprecated use {@link #indexScan_Default(IndexRowType, IndexKeyRange, Ordering, IndexScanSelector)}
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static Operator indexScan_Default(IndexRowType indexType, boolean reverse)
    {
        return indexScan_Default(indexType, reverse, IndexKeyRange.unbounded(indexType));
    }

    /**
     * Creates a scan operator for the given index, using LEFT JOIN semantics after the indexType's tableType.
     * @param indexType the index to scan
     * @param reverse whether to scan in reverse order
     * @param indexKeyRange the scan range
     * @return the scan operator
     * @deprecated use {@link #indexScan_Default(IndexRowType, IndexKeyRange, Ordering, IndexScanSelector)}
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static Operator indexScan_Default(IndexRowType indexType, boolean reverse, IndexKeyRange indexKeyRange)
    {
        if (indexKeyRange == null) {
            indexKeyRange = IndexKeyRange.unbounded(indexType);
        }
        return indexScan_Default(indexType, reverse, indexKeyRange, indexType.tableType());
    }

    /**
     * Creates a scan operator for the given index, using LEFT JOIN semantics after the given table type.
     * @param indexType the index to scan
     * @param reverse whether to scan in reverse order
     * @param indexKeyRange the scan range
     * @param innerJoinUntilRowType the table after which the scan should start using LEFT JOIN GI semantics.
     * @return the scan operator
     * @deprecated use {@link #indexScan_Default(IndexRowType, IndexKeyRange, Ordering, IndexScanSelector)}
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static Operator indexScan_Default(IndexRowType indexType,
                                             boolean reverse,
                                             IndexKeyRange indexKeyRange,
                                             TableRowType innerJoinUntilRowType)
    {
        Ordering ordering = new Ordering();
        int fields = indexType.nFields();
        for (int f = 0; f < fields; f++) {
            ordering.append(new TPreparedField(indexType.typeAt(f), f), !reverse);
        }
        return indexScan_Default(indexType, indexKeyRange, ordering, innerJoinUntilRowType);
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             Ordering ordering)
    {
        return indexScan_Default(indexType, indexKeyRange, ordering, indexType.tableType());
    }

    /**
     * Creates a scan operator for the given index, using LEFT JOIN semantics after the given table type.
     * @param indexType the index to scan
     * @param reverse whether to scan in reverse order
     * @param indexKeyRange the scan range
     * @param indexScanSelector
     * @return the scan operator
     * @deprecated use {@link #indexScan_Default(IndexRowType, IndexKeyRange, Ordering, IndexScanSelector)}
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static Operator indexScan_Default(IndexRowType indexType,
                                             boolean reverse,
                                             IndexKeyRange indexKeyRange,
                                             IndexScanSelector indexScanSelector)
    {
        Ordering ordering = new Ordering();
        int fields = indexType.nFields();
        for (int f = 0; f < fields; f++) {
            ordering.append(new TPreparedField(indexType.typeAt(f), f), !reverse);
        }
        return indexScan_Default(indexType, indexKeyRange, ordering, indexScanSelector);
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             Ordering ordering,
                                             TableRowType innerJoinUntilRowType)
    {
        return indexScan_Default(indexType,
                                 indexKeyRange,
                                 ordering,
                                 IndexScanSelector.leftJoinAfter(indexType.index(),
                                                                 innerJoinUntilRowType.table()));
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             Ordering ordering,
                                             IndexScanSelector indexScanSelector)
    {
        return indexScan_Default(indexType, indexKeyRange, ordering, indexScanSelector, 1);
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             int lookaheadQuantum)
    {
        Ordering ordering = new Ordering();
        int fields = indexType.nFields();
        for (int f = 0; f < fields; f++) {
            ordering.append(new TPreparedField(indexType.typeAt(f), f), true);
        }
        IndexScanSelector indexScanSelector = IndexScanSelector.leftJoinAfter(indexType.index(),
                                                                              indexType.tableType().table());
        return indexScan_Default(indexType, indexKeyRange, ordering, indexScanSelector, lookaheadQuantum);
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             Ordering ordering,
                                             IndexScanSelector indexScanSelector,
                                             int lookaheadQuantum)
    {
        return new IndexScan_Default(indexType, indexKeyRange, ordering, indexScanSelector, lookaheadQuantum);
    }

    // Select

    public static Operator select_HKeyOrdered(Operator inputOperator,
                                              RowType predicateRowType,
                                              TPreparedExpression predicate)
    {
        return new Select_HKeyOrdered(inputOperator, predicateRowType, predicate);
    }

    public static Operator select_HKeyOrdered(Operator inputOperator,
                                              RowType predicateRowType,
                                              ExpressionGenerator predicate)
    {
        return new Select_HKeyOrdered(inputOperator, predicateRowType, predicate.getTPreparedExpression());
    }

    // Filter

    public static Operator filter_Default(Operator inputOperator, Collection<? extends RowType> keepTypes)
    {
        return new Filter_Default(inputOperator, keepTypes);
    }

    // Product

    /** deprecated */
    public static Operator product_NestedLoops(Operator outerInput,
                                                       Operator innerInput,
                                                       RowType outerType,
                                                       RowType innerType,
                                                       int inputBindingPosition)
    {
        return product_NestedLoops(outerInput, innerInput, outerType, null, innerType, inputBindingPosition);
    }

    /** deprecated */
    public static Operator product_NestedLoops(Operator outerInput,
                                                       Operator innerInput,
                                                       RowType outerType,
                                                       TableRowType branchType,
                                                       RowType innerType,
                                                       int inputBindingPosition)
    {
        return map_NestedLoops(outerInput,
                               product_Nested(innerInput, outerType, branchType, innerType, inputBindingPosition),
                               inputBindingPosition,
                               false, 0);
    }

    public static Operator product_Nested(Operator input,
                                          RowType outerType,
                                          TableRowType branchType,
                                          RowType inputType,
                                          int bindingPosition)
    {
        return new Product_Nested(input, outerType, branchType, inputType, bindingPosition);
    }

    // Count

    public static Operator count_Default(Operator input,
                                         RowType countType)
    {
        return new Count_Default(input, countType);
    }

    public static Operator count_TableStatus(RowType tableType)
    {
        return new Count_TableStatus(tableType);
    }

    // Sort

    public static Operator sort_InsertionLimited(Operator inputOperator,
                                                 RowType sortType,
                                                 Ordering ordering,
                                                 SortOption sortOption,
                                                 int limit)
    {
        return new Sort_InsertionLimited(inputOperator, sortType, ordering, sortOption, limit);
    }

    public static Operator sort_General(Operator inputOperator,
                                        RowType sortType,
                                        Ordering ordering,
                                        SortOption sortOption)
    {
        return new Sort_General(inputOperator, sortType, ordering, sortOption);
    }

    public static Ordering ordering()
    {
        return new Ordering();
    }

    // Distinct
    public static Operator distinct_Partial(Operator input, RowType distinctType)
    {
        return new Distinct_Partial(input, distinctType, null);
    }

    public static Operator distinct_Partial(Operator input,
                                            RowType distinctType,
                                            List<AkCollator> collators)
    {
        return new Distinct_Partial(input, distinctType, collators);
    }

    // Map

    public static Operator map_NestedLoops(Operator outerInput,
                                           Operator innerInput,
                                           int inputBindingPosition,
                                           boolean pipeline,
                                           int depth)
    {
        return new Map_NestedLoops(outerInput, innerInput, inputBindingPosition, 
                                   pipeline, depth);
    }

    // IfEmpty

    public static Operator ifEmpty_Default(Operator input, RowType rowType,
                                           List<? extends TPreparedExpression> pExpressions,
                                           InputPreservationOption inputPreservation)
    {
        return new IfEmpty_Default(input, rowType, pExpressions, inputPreservation);
    }

    public static Operator ifEmpty_DefaultTest(Operator input, RowType rowType,
                                           List<? extends ExpressionGenerator> expressions,
                                           InputPreservationOption inputPreservation)
    {
        return new IfEmpty_Default(input, rowType, generateNew(expressions), inputPreservation);
    }

    // Union

    public static Operator unionAll_Default(Operator input1,
                                            RowType input1RowType,
                                            Operator input2,
                                            RowType input2RowType,
                                            boolean openBoth)
    {
        return new UnionAll_Default(input1, input1RowType, input2, input2RowType, openBoth);
    }

    // Except

    public static Operator except_Ordered(Operator leftInput, Operator rightInput,
                                          RowType leftRowType, RowType rightRowType,
                                          int leftOrderingFields,
                                          int rightOrderingFields,
                                          boolean[] ascending,
                                          boolean removeDuplicates)
    {
        return new Except_Ordered(leftInput, rightInput,
                leftRowType, rightRowType,
                leftOrderingFields,
                rightOrderingFields,
                ascending,
                removeDuplicates);
    }

    // Intersect

    public static Operator intersect_Ordered(Operator leftInput, Operator rightInput,
                                             RowType leftRowType, RowType rightRowType,
                                             int leftOrderingFields,
                                             int rightOrderingFields,
                                             boolean[] ascending)
    {
        return new Intersect_Ordered(leftInput, rightInput,
                leftRowType, rightRowType,
                leftOrderingFields,
                rightOrderingFields,
                ascending,
                JoinType.INNER_JOIN,
                EnumSet.of(IntersectOption.OUTPUT_LEFT),
                null,
                false);
    }
    
    // Intersect
    
    public static Operator intersect_Ordered(Operator leftInput, Operator rightInput,
                                                RowType leftRowType, RowType rightRowType,
                                                int leftOrderingFields,
                                                int rightOrderingFields,
                                                int comparisonFields,
                                                JoinType joinType,
                                                IntersectOption intersectOutput,
                                                List<TComparison> comparisons)
    {
        if (comparisonFields < 0) {
            throw new IllegalArgumentException();
        }
        boolean[] ascending = new boolean[comparisonFields];
        Arrays.fill(ascending, true);
        return new Intersect_Ordered(leftInput, rightInput,
                                     leftRowType, rightRowType,
                                     leftOrderingFields,
                                     rightOrderingFields,
                                     ascending,
                                     joinType,
                                     EnumSet.of(intersectOutput),
                                     comparisons,
                                     false);
    }

    public static Operator intersect_Ordered(Operator leftInput, Operator rightInput,
                                                RowType leftRowType, RowType rightRowType,
                                                int leftOrderingFields,
                                                int rightOrderingFields,
                                                boolean[] ascending,
                                                JoinType joinType,
                                                EnumSet<IntersectOption> intersectOptions,
                                                List<TComparison> comparisons)
    {
        return new Intersect_Ordered(leftInput, rightInput,
                                     leftRowType, rightRowType,
                                     leftOrderingFields,
                                     rightOrderingFields,
                                     ascending,
                                     joinType,
                                     intersectOptions,
                                     comparisons,
                                     false);
    }

    public static Operator intersect_Ordered(Operator leftInput, Operator rightInput,
                                                RowType leftRowType, RowType rightRowType,
                                                int leftOrderingFields,
                                                int rightOrderingFields,
                                                int comparisonFields,
                                                JoinType joinType,
                                                IntersectOption intersectOutput,
                                                List<TComparison> comparisons,
                                                boolean outputEqual)
    {
        if (comparisonFields < 0) {
            throw new IllegalArgumentException();
        }
        boolean[] ascending = new boolean[comparisonFields];
        Arrays.fill(ascending, true);
        return new Intersect_Ordered(leftInput, rightInput,
                leftRowType, rightRowType,
                leftOrderingFields,
                rightOrderingFields,
                ascending,
                joinType,
                EnumSet.of(intersectOutput),
                comparisons,
                outputEqual);
    }

    public static Operator intersect_Ordered(Operator leftInput, Operator rightInput,
                                                RowType leftRowType, RowType rightRowType,
                                                int leftOrderingFields,
                                                int rightOrderingFields,
                                                boolean[] ascending,
                                                JoinType joinType,
                                                EnumSet<IntersectOption> intersectOptions,
                                                List<TComparison> comparisons,
                                                boolean outputEqual)
    {
        return new Intersect_Ordered(leftInput, rightInput,
                leftRowType, rightRowType,
                leftOrderingFields,
                rightOrderingFields,
                ascending,
                joinType,
                intersectOptions,
                comparisons,
                outputEqual);
    }

    // Union

    public static Operator union_Ordered(Operator leftInput, Operator rightInput,
                                         RowType leftRowType, RowType rightRowType,
                                         int leftOrderingFields,
                                         int rightOrderingFields,
                                         boolean[] ascending,
                                         boolean outputEqual)
    {
        return new Union_Ordered(leftInput, rightInput,
                                 leftRowType, rightRowType,
                                 leftOrderingFields,
                                 rightOrderingFields,
                                 ascending, outputEqual);
    }

    // HKeyUnion

    public static Operator hKeyUnion_Ordered(Operator leftInput, Operator rightInput,
                                             RowType leftRowType, RowType rightRowType,
                                             int leftOrderingFields, int rightOrderingFields,
                                             int comparisonFields,
                                             TableRowType outputHKeyTableRowType)
    {
        return new HKeyUnion_Ordered(leftInput, rightInput,
                                     leftRowType, rightRowType,
                                     leftOrderingFields, rightOrderingFields,
                                     comparisonFields,
                                     outputHKeyTableRowType);
    }

    // Using_BloomFilter

    public static Operator using_BloomFilter(Operator filterInput,
                                             RowType filterRowType,
                                             long estimatedRowCount,
                                             int filterBindingPosition,
                                             Operator streamInput)
    {
        return new Using_BloomFilter(filterInput,
                                     filterRowType,
                                     estimatedRowCount,
                                     filterBindingPosition,
                                     streamInput,
                                     null);
    }

    public static Operator using_BloomFilter(Operator filterInput,
                                             RowType filterRowType,
                                             long estimatedRowCount,
                                             int filterBindingPosition,
                                             Operator streamInput,
                                             List<AkCollator> collators)
    {
        return new Using_BloomFilter(filterInput,
                                     filterRowType,
                                     estimatedRowCount,
                                     filterBindingPosition,
                                     streamInput,
                                     collators);
    }

    // Select_BloomFilter

    public static Operator select_BloomFilterTest(Operator input,
                                              Operator onPositive,
                                              List<? extends ExpressionGenerator> filterFields,
                                              int bindingPosition,
                                              boolean pipeline,
                                              int depth)
    {
        return select_BloomFilter(input, onPositive, generateNew(filterFields), null, bindingPosition, pipeline, depth);
    }

    public static Operator select_BloomFilter(Operator input,
                                              Operator onPositive,
                                              List<? extends TPreparedExpression> tFilterFields,
                                              int bindingPosition,
                                              boolean pipeline,
                                              int depth)
    {
        return select_BloomFilter(input, onPositive, tFilterFields, null, bindingPosition, pipeline, depth);
    }

    public static Operator select_BloomFilter(Operator input,
                                              Operator onPositive,
                                              List<? extends TPreparedExpression> tFilterFields,
                                              List<AkCollator> collators,
                                              int bindingPosition,
                                              boolean pipeline,
                                              int depth)
    {
        return new Select_BloomFilter(input,
                                      onPositive,
                                      tFilterFields,
                                      collators,
                                      bindingPosition,
                                      pipeline,
                                      depth);
    }

    public static Operator select_BloomFilter(Operator input,
                                              Operator onPositive,
                                              List<? extends ExpressionGenerator> filterFields,
                                              List<AkCollator> collators,
                                              int bindingPosition,
                                              boolean pipeline,
                                              int depth,
                                              ExpressionGenerator.ErasureMaker marker)
    {
        return new Select_BloomFilter(input,
                onPositive,
                generateNew(filterFields),
                collators,
                bindingPosition,
                pipeline,
                depth);
    }

    // EmitBoundRow_Nested

    public static Operator emitBoundRow_Nested(Operator input,
                                               RowType inputRowType,
                                               RowType outputRowType,
                                               RowType boundRowType,
                                               int bindingPosition)
    {
        return new EmitBoundRow_Nested(input,
                                       inputRowType, outputRowType, boundRowType,
                                       bindingPosition);
    }

    // Insert
    public static UpdatePlannable insert_Default(Operator inputOperator)
    {
        return new Insert_Default(inputOperator);
    }

    public static Operator insert_Returning (Operator inputOperator)
    {
        return new Insert_Returning(inputOperator);
    }

    // Update

    public static UpdatePlannable update_Default(Operator inputOperator,
                                                 UpdateFunction updateFunction)
    {
        return new Update_Default(inputOperator, updateFunction);
    }
    
    public static Operator update_Returning (Operator inputOperator,
                                            UpdateFunction updateFunction)
    {
        return new Update_Returning (inputOperator, updateFunction);
    }
    
    // Delete

    public static UpdatePlannable delete_Default(Operator inputOperator)
    {
        return new Delete_Default(inputOperator);
    }

    public static Operator delete_Returning (Operator inputOperator, boolean cascadeDelete)
    {
        return new Delete_Returning(inputOperator, cascadeDelete);
    }

    // Buffer

    public static Operator buffer_Default(Operator inputOperator, RowType inputRowType)
    {
        return new Buffer_Default(inputOperator, inputRowType);
    }

    public static Operator hKeyRow_Default(RowType rowType,
                                           List<? extends TPreparedExpression> pExpressions)
    {
        return new HKeyRow_Default(rowType, pExpressions);
    }

    public static Operator hKeyRow_DefaultTest(RowType rowType,
                                               List<ExpressionGenerator> generators)
    {
        return new HKeyRow_Default(rowType, generateNew(generators));
    }

    // Execution interface

    public static Cursor cursor(Operator root, QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new ChainedCursor(context, root.cursor(context, bindingsCursor));
    }

    public static Cursor cursor(Operator root, QueryContext context, QueryBindings bindings)
    {
        return cursor(root, context, new SingletonQueryBindingsCursor(bindings));
    }

    // Options

    // Flattening flags

    public static enum JoinType {
        INNER_JOIN,
        LEFT_JOIN,
        RIGHT_JOIN,
        FULL_JOIN
    }

    public static enum FlattenOption {
        KEEP_PARENT,
        KEEP_CHILD,
        LEFT_JOIN_SHORTENS_HKEY
    }

    // Lookup flags

    public static enum InputPreservationOption
    {
        KEEP_INPUT,
        DISCARD_INPUT
    }

    // Sort flags

    public static enum SortOption {
        PRESERVE_DUPLICATES,
        SUPPRESS_DUPLICATES
    }

    // Intersect output flags

    public static enum IntersectOption
    {
        OUTPUT_LEFT,
        OUTPUT_RIGHT,
        SEQUENTIAL_SCAN,
        SKIP_SCAN
    }

    // Ordering specification

    public static class Ordering
    {
        public String toString()
        {
            StringBuilder buffer = new StringBuilder();
            buffer.append('(');
            List<?> exprs = expressions;
            for (int i = 0, size = sortColumns(); i < size; i++) {
                if (i > 0) {
                    buffer.append(", ");
                }
                buffer.append(exprs.get(i));
                buffer.append(' ');
                buffer.append(directions.get(i) ? "ASC" : "DESC");
            }
            buffer.append(')');
            return buffer.toString();
        }

        public int sortColumns()
        {
            return expressions.size();
        }

        public TPreparedExpression expression(int i) {
            return expressions.get(i);
        }

        public TInstance type(int i) {
            return expressions.get(i).resultType();
        }

        public boolean ascending(int i)
        {
            return directions.get(i);
        }

        public boolean allAscending()
        {
            boolean allAscending = true;
            for (Boolean direction : directions) {
                if (!direction) {
                    allAscending = false;
                }
            }
            return allAscending;
        }

        public boolean allDescending()
        {
            boolean allDescending = true;
            for (Boolean direction : directions) {
                if (direction) {
                    allDescending = false;
                }
            }
            return allDescending;
        }

        public AkCollator collator(int i)
        {
            return collators.get(i);
        }

        public void append(ExpressionGenerator expressionGenerator, boolean ascending)
        {
            TPreparedExpression newExpr;
            newExpr = expressionGenerator.getTPreparedExpression();
            append(newExpr, ascending);
        }

        public void append(TPreparedExpression tExpression, boolean ascending)
        {
            append(tExpression, ascending, null);
        }
        
        public void append(ExpressionGenerator expression, boolean ascending, AkCollator collator)
        {
            TPreparedExpression newStyle;
            newStyle = expression.getTPreparedExpression();
            append(newStyle, ascending, collator);
        }

        public void append(TPreparedExpression tExpression,  boolean ascending,
                           AkCollator collator)
        {
            expressions.add(tExpression);
            directions.add(ascending);
            collators.add(collator);
        }

        public Ordering copy()
        {
            Ordering copy = new Ordering();
            copy.expressions.addAll(expressions);
            copy.directions.addAll(directions);
            copy.collators.addAll(collators);
            return copy;
        }

        public Ordering() {
            expressions = new ArrayList<>();
        }

        private final List<TPreparedExpression> expressions;
        private final List<Boolean> directions = new ArrayList<>(); // true: ascending, false: descending
        private final List<AkCollator> collators = new ArrayList<>();
    }

}
