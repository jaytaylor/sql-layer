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

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.aggregation.AggregatorRegistry;
import com.akiban.server.aggregation.Aggregators;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.texpressions.TPreparedExpression;

import java.util.*;

public class API
{
    // Aggregate

    public static Operator aggregate_Partial(Operator inputOperator,
                                                     RowType rowType,
                                                     int inputsIndex,
                                                     AggregatorRegistry registry,
                                                     List<String> aggregatorNames)
    {
        return new Aggregate_Partial(
                inputOperator, rowType,
                inputsIndex,
                Aggregators.factories(
                        registry,
                        Aggregators.aggregatorIds(aggregatorNames, rowType, inputsIndex)
                )
        );
    }

    public static Operator aggregate_Partial(Operator inputOperator,
                                             RowType rowType,
                                             int inputsIndex,
                                             List<? extends TAggregator> aggregatorFactories,
                                             List<? extends TInstance> aggregatorTypes
                                             )
    {
        return new Aggregate_Partial(inputOperator, rowType, inputsIndex, aggregatorFactories, aggregatorTypes);
    }

    // Project

    public static Operator project_Default(Operator inputOperator,
                                           RowType rowType,
                                           List<Expression> projections)
    {
        return new Project_Default(inputOperator, rowType, projections, null);
    }

    public static Operator project_Default(Operator inputOperator,
                                                   RowType rowType,
                                                   List<Expression> projections,
                                                   List<? extends TPreparedExpression> pExpressions)
    {
        return new Project_Default(inputOperator, rowType, projections, pExpressions);
    }
    
    public static Operator project_Table(Operator inputOperator,
                                                 RowType inputRowType,
                                                 RowType outputRowType,
                                                 List<Expression> projections,
                                                 List<? extends TPreparedExpression> pExpressions)
    {
        return new Project_Default (inputOperator, inputRowType, outputRowType, projections, pExpressions);
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

    public static Operator groupScan_Default(GroupTable groupTable)
    {
        return new GroupScan_Default(new GroupScan_Default.FullGroupCursorCreator(groupTable));
    }

    public static Operator groupScan_Default(GroupTable groupTable,
                                             int hKeyBindingPosition,
                                             boolean deep,
                                             UserTable hKeyType,
                                             UserTable shortenUntil)
    {
        return new GroupScan_Default(
                new GroupScan_Default.PositionalGroupCursorCreator(groupTable, hKeyBindingPosition, deep, hKeyType, shortenUntil));
    }

    // ValuesScan

    public static Operator valuesScan_Default (Collection<? extends BindableRow> rows, RowType rowType)
    {
        return new ValuesScan_Default (rows, rowType);
    }
    
    // BranchLookup

    public static Operator branchLookup_Default(Operator inputOperator,
                                                GroupTable groupTable,
                                                RowType inputRowType,
                                                UserTableRowType outputRowType,
                                                InputPreservationOption flag)
    {
        return branchLookup_Default(inputOperator, groupTable, inputRowType, outputRowType, flag, NO_LIMIT);
    }

    public static Operator branchLookup_Default(Operator inputOperator,
                                                GroupTable groupTable,
                                                RowType inputRowType,
                                                UserTableRowType outputRowType,
                                                InputPreservationOption flag,
                                                Limit limit)
    {
        return new BranchLookup_Default(inputOperator, groupTable, inputRowType, outputRowType, flag, limit);
    }

    /** deprecated */
    public static Operator branchLookup_Nested(GroupTable groupTable,
                                               RowType inputRowType,
                                               UserTableRowType outputRowType,
                                               InputPreservationOption flag,
                                               int inputBindingPosition)
    {
        return new BranchLookup_Nested(groupTable,
                                       inputRowType,
                                       null,
                                       outputRowType,
                                       flag,
                                       inputBindingPosition);
    }

    public static Operator branchLookup_Nested(GroupTable groupTable,
                                               RowType inputRowType,
                                               UserTableRowType ancestorRowType,
                                               UserTableRowType outputRowType,
                                               InputPreservationOption flag,
                                               int inputBindingPosition)
    {
        return new BranchLookup_Nested(groupTable,
                                       inputRowType,
                                       ancestorRowType,
                                       outputRowType,
                                       flag,
                                       inputBindingPosition);
    }

    // Limit

    public static Operator limit_Default(Operator inputOperator, int limitRows)
    {
        return limit_Default(inputOperator, limitRows, Types3Switch.ON);
    }

    public static Operator limit_Default(Operator inputOperator, int limitRows, boolean usePVals)
    {
        return new Limit_Default(inputOperator, limitRows, usePVals);
    }

    public static Operator limit_Default(Operator inputOperator,
                                         int skipRows,
                                         boolean skipIsBinding,
                                         int limitRows,
                                         boolean limitIsBinding)
    {
        return limit_Default(inputOperator, skipRows, skipIsBinding, limitRows, limitIsBinding, Types3Switch.ON);
    }
    public static Operator limit_Default(Operator inputOperator,
                                                 int skipRows,
                                                 boolean skipIsBinding,
                                                 int limitRows,
                                                 boolean limitIsBinding,
                                                 boolean usePVals)
    {
        return new Limit_Default(inputOperator, skipRows, skipIsBinding, limitRows, limitIsBinding, usePVals);
    }

    // AncestorLookup

    public static Operator ancestorLookup_Default(Operator inputOperator,
                                                  GroupTable groupTable,
                                                  RowType rowType,
                                                  Collection<UserTableRowType> ancestorTypes,
                                                  InputPreservationOption flag)
    {
        return new AncestorLookup_Default(inputOperator, groupTable, rowType, ancestorTypes, flag);
    }

    public static Operator ancestorLookup_Nested(GroupTable groupTable,
                                                 RowType rowType,
                                                 Collection<UserTableRowType> ancestorTypes,
                                                 int hKeyBindingPosition)
    {
        return new AncestorLookup_Nested(groupTable, rowType, ancestorTypes, hKeyBindingPosition);
    }

    // IndexScan

    /**
     * Creates a full ascending scan operator for the given index using LEFT JOIN semantics after the indexType's
     * tableType
     * @param indexType the index to scan
     * @return the scan operator
     * @deprecated use {@link #indexScan_Default(IndexRowType, IndexKeyRange, Ordering, IndexScanSelector, boolean)}
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static Operator indexScan_Default(IndexRowType indexType)
    {
        boolean usePValues = Types3Switch.ON;
        return indexScan_Default(indexType, false, IndexKeyRange.unbounded(indexType, usePValues), usePValues);
    }

    /**
     * Creates a full ascending scan operator for the given index using LEFT JOIN semantics after the indexType's
     * tableType
     * @param indexType the index to scan
     * @param reverse whether to scan in reverse order
     * @return the scan operator
     * @deprecated use {@link #indexScan_Default(IndexRowType, IndexKeyRange, Ordering, IndexScanSelector, boolean)}
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static Operator indexScan_Default(IndexRowType indexType, boolean reverse)
    {
        boolean usePValues = Types3Switch.ON;
        return indexScan_Default(indexType, reverse, IndexKeyRange.unbounded(indexType, usePValues), usePValues);
    }

    public static Operator indexScan_Default(IndexRowType indexType, boolean reverse, IndexKeyRange indexKeyRange)
    {
        return indexScan_Default(indexType, reverse, indexKeyRange, Types3Switch.ON);
    }
    /**
     * Creates a scan operator for the given index, using LEFT JOIN semantics after the indexType's tableType.
     * @param indexType the index to scan
     * @param reverse whether to scan in reverse order
     * @param indexKeyRange the scan range
     * @return the scan operator
     * @deprecated use {@link #indexScan_Default(IndexRowType, IndexKeyRange, Ordering, IndexScanSelector, boolean)}
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static Operator indexScan_Default(IndexRowType indexType, boolean reverse, IndexKeyRange indexKeyRange, boolean usePValues)
    {
        if (indexKeyRange == null) {
            indexKeyRange = IndexKeyRange.unbounded(indexType, usePValues);
        }
        return indexScan_Default(indexType, reverse, indexKeyRange, indexType.tableType(), usePValues);
    }
    public static Operator indexScan_Default(IndexRowType indexType,
                                             boolean reverse,
                                             IndexKeyRange indexKeyRange,
                                             UserTableRowType innerJoinUntilRowType)
    {
        return indexScan_Default(indexType, reverse, indexKeyRange, innerJoinUntilRowType, Types3Switch.ON);
    }

    /**
     * Creates a scan operator for the given index, using LEFT JOIN semantics after the given table type.
     * @param indexType the index to scan
     * @param reverse whether to scan in reverse order
     * @param indexKeyRange the scan range
     * @param innerJoinUntilRowType the table after which the scan should start using LEFT JOIN GI semantics.
     * @return the scan operator
     * @deprecated use {@link #indexScan_Default(IndexRowType, IndexKeyRange, Ordering, IndexScanSelector, boolean)}
     */
    @Deprecated
    public static Operator indexScan_Default(IndexRowType indexType,
                                             boolean reverse,
                                             IndexKeyRange indexKeyRange,
                                             UserTableRowType innerJoinUntilRowType,
                                             boolean usePVals)
    {
        Ordering ordering = new Ordering();
        int fields = indexType.nFields();
        for (int f = 0; f < fields; f++) {
            ordering.append(new FieldExpression(indexType, f), !reverse);
        }
        return indexScan_Default(indexType, indexKeyRange, ordering, innerJoinUntilRowType, usePVals);
    }

    /**
     * Creates a scan operator for the given index, using LEFT JOIN semantics after the given table type.
     * @param indexType the index to scan
     * @param reverse whether to scan in reverse order
     * @param indexKeyRange the scan range
     * @param indexScanSelector
     * @return the scan operator
     * @deprecated use {@link #indexScan_Default(IndexRowType, IndexKeyRange, Ordering, IndexScanSelector, boolean)}
     */
    @Deprecated
    public static Operator indexScan_Default(IndexRowType indexType,
                                             boolean reverse,
                                             IndexKeyRange indexKeyRange,
                                             IndexScanSelector indexScanSelector,
                                             boolean usePVals)
    {
        Ordering ordering = new Ordering();
        int fields = indexType.nFields();
        for (int f = 0; f < fields; f++) {
            ordering.append(new FieldExpression(indexType, f), !reverse);
        }
        return indexScan_Default(indexType, indexKeyRange, ordering, indexScanSelector, usePVals);
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             Ordering ordering)
    {
        return indexScan_Default(indexType, indexKeyRange, ordering, Types3Switch.ON);
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             Ordering ordering,
                                             boolean usePVals)
    {
        return indexScan_Default(indexType, indexKeyRange, ordering, indexType.tableType(), usePVals);
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             Ordering ordering,
                                             UserTableRowType innerJoinUntilRowType)
    {
        return indexScan_Default(indexType, indexKeyRange, ordering, innerJoinUntilRowType, Types3Switch.ON);
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             Ordering ordering,
                                             UserTableRowType innerJoinUntilRowType,
                                             boolean usePVals)
    {
        return indexScan_Default(indexType,
                                 indexKeyRange,
                                 ordering,
                                 IndexScanSelector.leftJoinAfter(indexType.index(),
                                                                 innerJoinUntilRowType.userTable()), usePVals);
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             Ordering ordering,
                                             IndexScanSelector indexScanSelector)
    {
        return new IndexScan_Default(indexType, indexKeyRange, ordering, indexScanSelector, Types3Switch.ON);
    }


    public static Operator indexScan_Default(IndexRowType indexType,
                                             IndexKeyRange indexKeyRange,
                                             Ordering ordering,
                                             IndexScanSelector indexScanSelector,
                                             boolean usePVals)
    {
        return new IndexScan_Default(indexType, indexKeyRange, ordering, indexScanSelector, usePVals);
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
                                                      Expression predicate)
    {
        return new Select_HKeyOrdered(inputOperator, predicateRowType, predicate);
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
        return new Product_NestedLoops(outerInput, innerInput, outerType, null, innerType, inputBindingPosition);
    }

    public static Operator product_NestedLoops(Operator outerInput,
                                                       Operator innerInput,
                                                       RowType outerType,
                                                       UserTableRowType branchType,
                                                       RowType innerType,
                                                       int inputBindingPosition)
    {
        return new Product_NestedLoops(outerInput, innerInput, outerType, branchType, innerType, inputBindingPosition);
    }

    // Count

    public static Operator count_Default(Operator input,
                                         RowType countType)
    {
        return new Count_Default(input, countType, Types3Switch.ON);
    }

    public static Operator count_Default(Operator input,
                                         RowType countType,
                                         boolean usePValues)
    {
        return new Count_Default(input, countType, usePValues);
    }

    public static Operator count_TableStatus(RowType tableType)
    {
        return count_TableStatus(tableType, Types3Switch.ON);
    }


    public static Operator count_TableStatus(RowType tableType, boolean usePValues)
    {
        return new Count_TableStatus(tableType, usePValues);
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

    public static Operator sort_Tree(Operator inputOperator,
                                     RowType sortType,
                                     Ordering ordering,
                                     SortOption sortOption)
    {
        return new Sort_Tree(inputOperator, sortType, ordering, sortOption, Types3Switch.ON);
    }

    public static Operator sort_Tree(Operator inputOperator, 
                                     RowType sortType, 
                                     Ordering ordering, 
                                     SortOption sortOption,
                                     boolean usePVals)
    {
        return new Sort_Tree(inputOperator, sortType, ordering, sortOption, usePVals);
    }

    public static Ordering ordering()
    {
        return new Ordering();
    }

    // Distinct
    public static Operator distinct_Partial(Operator input, RowType distinctType)
    {
        return new Distinct_Partial(input, distinctType, null, Types3Switch.ON);
    }

    public static Operator distinct_Partial(Operator input,
                                            RowType distinctType,
                                            List<AkCollator> collators,
                                            boolean usePValues)
    {
        return new Distinct_Partial(input, distinctType, collators, usePValues);
    }

    // Map

    public static Operator map_NestedLoops(Operator outerInput,
                                           Operator innerInput,
                                           int inputBindingPosition)
    {
        return new Map_NestedLoops(outerInput, innerInput, inputBindingPosition);
    }

    // IfEmpty

    public static Operator ifEmpty_Default(Operator input, RowType rowType,
                                           List<? extends Expression> expressions,
                                           InputPreservationOption inputPreservation)
    {
        return new IfEmpty_Default(input, rowType, expressions, inputPreservation);
    }

    // Union

    public static Operator unionAll(Operator input1, RowType input1RowType, Operator input2, RowType input2RowType)
    {
        return unionAll(input1, input1RowType, input2, input2RowType, Types3Switch.ON);
    }

    public static Operator unionAll(Operator input1, RowType input1RowType, Operator input2, RowType input2RowType,
                                    boolean usePvalues)
    {
        return new UnionAll_Default(input1, input1RowType, input2, input2RowType, usePvalues);
    }
    
    // Intersect
    
    /** deprecated */

    public static Operator intersect_Ordered(Operator leftInput, Operator rightInput,
                                             IndexRowType leftRowType, IndexRowType rightRowType,
                                             int leftOrderingFields,
                                             int rightOrderingFields,
                                             int comparisonFields,
                                             JoinType joinType,
                                             IntersectOption intersectOutput)
    {
        return intersect_Ordered(leftInput, rightInput,  leftRowType, rightRowType,
                leftOrderingFields, rightOrderingFields, comparisonFields, joinType, intersectOutput,
                Types3Switch.ON);
    }

    public static Operator intersect_Ordered(Operator leftInput, Operator rightInput,
                                            IndexRowType leftRowType, IndexRowType rightRowType,
                                            int leftOrderingFields,
                                            int rightOrderingFields,
                                            int comparisonFields,
                                            JoinType joinType,
                                            IntersectOption intersectOutput,
                                            boolean usePValues)
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
                                     usePValues);
    }

    public static Operator intersect_Ordered(Operator leftInput, Operator rightInput,
                                             IndexRowType leftRowType, IndexRowType rightRowType,
                                             int leftOrderingFields,
                                             int rightOrderingFields,
                                             boolean[] ascending,
                                             JoinType joinType,
                                             EnumSet<IntersectOption> intersectOptions)
    {
        return new Intersect_Ordered(leftInput, rightInput,
                leftRowType, rightRowType,
                leftOrderingFields,
                rightOrderingFields,
                ascending,
                joinType,
                intersectOptions,
                Types3Switch.ON);
    }

    public static Operator intersect_Ordered(Operator leftInput, Operator rightInput,
                                            IndexRowType leftRowType, IndexRowType rightRowType,
                                            int leftOrderingFields,
                                            int rightOrderingFields,
                                            boolean[] ascending,
                                            JoinType joinType,
                                            EnumSet<IntersectOption> intersectOptions,
                                            boolean usePValues)
    {
        return new Intersect_Ordered(leftInput, rightInput,
                                     leftRowType, rightRowType,
                                     leftOrderingFields,
                                     rightOrderingFields,
                                     ascending,
                                     joinType,
                                     intersectOptions,
                                     usePValues);
    }
    
    // Union

    public static Operator union_Ordered(Operator leftInput, Operator rightInput,
                                         IndexRowType leftRowType, IndexRowType rightRowType,
                                         int leftOrderingFields,
                                         int rightOrderingFields,
                                         boolean[] ascending)
    {
        return new Union_Ordered(leftInput, rightInput,
                leftRowType, rightRowType,
                leftOrderingFields,
                rightOrderingFields,
                ascending,
                Types3Switch.ON);
    }

    public static Operator union_Ordered(Operator leftInput, Operator rightInput,
                                          IndexRowType leftRowType, IndexRowType rightRowType,
                                          int leftOrderingFields,
                                          int rightOrderingFields,
                                          boolean[] ascending,
                                          boolean usePValues)
    {
        return new Union_Ordered(leftInput, rightInput,
                                 leftRowType, rightRowType,
                                 leftOrderingFields,
                                 rightOrderingFields,
                                 ascending,
                                 usePValues);
    }

    // HKeyUnion

    public static Operator hKeyUnion_Ordered(Operator leftInput, Operator rightInput,
                                             RowType leftRowType, RowType rightRowType,
                                             int leftOrderingFields, int rightOrderingFields,
                                             int comparisonFields,
                                             UserTableRowType outputHKeyTableRowType)
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
                                     null,
                                     Types3Switch.ON);
    }

    public static Operator using_BloomFilter(Operator filterInput,
                                             RowType filterRowType,
                                             long estimatedRowCount,
                                             int filterBindingPosition,
                                             Operator streamInput,
                                             List<AkCollator> collators,
                                             boolean usePValues)
    {
        return new Using_BloomFilter(filterInput,
                                     filterRowType,
                                     estimatedRowCount,
                                     filterBindingPosition,
                                     streamInput,
                                     collators,
                                     usePValues);
    }

    // Select_BloomFilter

    public static Operator select_BloomFilter(Operator input,
                                              Operator onPositive,
                                              List<? extends Expression> filterFields,
                                              int bindingPosition)
    {
        return select_BloomFilter(input, onPositive, filterFields, null, bindingPosition);
    }

    public static Operator select_BloomFilter(Operator input,
                                              Operator onPositive,
                                              List<? extends Expression> filterFields,
                                              List<AkCollator> collators,
                                              int bindingPosition)
    {
        return new Select_BloomFilter(input,
                                      onPositive,
                                      filterFields,
                                      collators,
                                      bindingPosition);
    }

    // Insert

    public static UpdatePlannable insert_Default(Operator inputOperator)
    {
        return new Insert_Default(inputOperator);
    }


    // Update

    public static UpdatePlannable update_Default(Operator inputOperator,
                                                 UpdateFunction updateFunction)
    {
        return new Update_Default(inputOperator, updateFunction);
    }
    
    // Delete

    public static UpdatePlannable delete_Default(Operator inputOperator)
    {
        return new Delete_Default(inputOperator);
    }

    // Execution interface

    public static Cursor cursor(Operator root, QueryContext context)
    {
        // if all they need is the wrapped cursor, create it directly
        return new ChainedCursor(context, root.cursor(context));
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
            for (int i = 0; i < expressions.size(); i++) {
                if (i > 0) {
                    buffer.append(", ");
                }
                buffer.append(expressions.get(i));
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

        public Expression expression(int i) {
            return expressions.get(i);
        }

        public AkType type(int i)
        {
            return expressions.get(i).valueType();
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

        public void append(Expression expression, boolean ascending)
        {
            append(expression, ascending, null);
        }

        public void append(Expression expression, boolean ascending, AkCollator collator)
        {
            expressions.add(expression);
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

        private final List<com.akiban.server.expression.Expression> expressions =
            new ArrayList<com.akiban.server.expression.Expression>();
        private final List<Boolean> directions = new ArrayList<Boolean>(); // true: ascending, false: descending
        private final List<AkCollator> collators = new ArrayList<AkCollator>();
    }

    // Class state

    private static final Limit NO_LIMIT = new Limit()
    {

        @Override
        public boolean limitReached(RowBase row)
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "NO LIMIT";
        }

    };
}
