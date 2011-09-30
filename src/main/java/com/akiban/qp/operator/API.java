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

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;

import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;

import java.util.*;

public class API
{
    // Aggregate

    public static Operator aggregate_Partial(Operator inputOperator,
                                                     int inputsIndex,
                                                     AggregatorFactory factory,
                                                     List<String> aggregatorNames)
    {
        return new Aggregate_Partial(inputOperator, inputsIndex, factory, aggregatorNames);
    }

    // Project

    public static Operator project_Default(Operator inputOperator,
                                                   RowType rowType,
                                                   List<Expression> projections)
    {
        return new Project_Default(inputOperator, rowType, projections);
    }
    
    public static Operator project_Table(Operator inputOperator,
                                                 RowType inputRowType,
                                                 RowType outputRowType,
                                                 List<Expression> projections)
    {
        return new Project_Default (inputOperator, inputRowType, outputRowType, projections);
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
        return groupScan_Default(groupTable, NO_LIMIT);
    }

    public static Operator groupScan_Default(GroupTable groupTable, Limit limit)
    {
        return new GroupScan_Default(new GroupScan_Default.FullGroupCursorCreator(groupTable), limit);
    }

    public static Operator groupScan_Default(GroupTable groupTable,
                                                     Limit limit,
                                                     int hKeyBindingPosition,
                                                     boolean deep)
    {
        return new GroupScan_Default(
                new GroupScan_Default.PositionalGroupCursorCreator(groupTable, hKeyBindingPosition, deep),
                limit
        );
    }

    public static Operator valuesScan_Default (Collection<? extends Row> rows, RowType rowType)
    {
        return new ValuesScan_Default (rows, rowType);
    }
    // BranchLookup

    public static Operator branchLookup_Default(Operator inputOperator,
                                                        GroupTable groupTable,
                                                        RowType inputRowType,
                                                        RowType outputRowType,
                                                        LookupOption flag)
    {
        return branchLookup_Default(inputOperator, groupTable, inputRowType, outputRowType, flag, NO_LIMIT);
    }

    public static Operator branchLookup_Default(Operator inputOperator,
                                                        GroupTable groupTable,
                                                        RowType inputRowType,
                                                        RowType outputRowType,
                                                        LookupOption flag,
                                                        Limit limit)
    {
        return new BranchLookup_Default(inputOperator, groupTable, inputRowType, outputRowType, flag, limit);
    }

    public static Operator branchLookup_Nested(GroupTable groupTable,
                                                       RowType inputRowType,
                                                       RowType outputRowType,
                                                       LookupOption flag,
                                                       int inputBindingPosition)
    {
        return new BranchLookup_Nested(groupTable, inputRowType, outputRowType, flag, inputBindingPosition);
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
                                                          GroupTable groupTable,
                                                          RowType rowType,
                                                          Collection<? extends RowType> ancestorTypes,
                                                          LookupOption flag)
    {
        return new AncestorLookup_Default(inputOperator, groupTable, rowType, ancestorTypes, flag);
    }

    public static Operator ancestorLookup_Nested(GroupTable groupTable,
                                                         RowType rowType,
                                                         Collection<? extends RowType> ancestorTypes,
                                                         int hKeyBindingPosition)
    {
        return new AncestorLookup_Nested(groupTable, rowType, ancestorTypes, hKeyBindingPosition);
    }

    // IndexScan

    public static Operator indexScan_Default(IndexRowType indexType)
    {
        return indexScan_Default(indexType, false, null, indexType.tableType());
    }

    public static Operator indexScan_Default(IndexRowType indexType, boolean reverse, IndexKeyRange indexKeyRange)
    {
        return indexScan_Default(indexType, reverse, indexKeyRange, indexType.tableType());
    }

    public static Operator indexScan_Default(IndexRowType indexType,
                                                     boolean reverse,
                                                     IndexKeyRange indexKeyRange,
                                                     UserTableRowType innerJoinUntilRowType)
    {
        return new IndexScan_Default(indexType, reverse, indexKeyRange, innerJoinUntilRowType);
    }

    // Select

    public static Operator select_HKeyOrdered(Operator inputOperator,
                                                      RowType predicateRowType,
                                                      Expression predicate)
    {
        return new Select_HKeyOrdered(inputOperator, predicateRowType, predicate);
    }

    // Filter

    public static Operator filter_Default(Operator inputOperator, Collection<RowType> keepTypes)
    {
        return new Filter_Default(inputOperator, keepTypes);
    }

    // Product

    /**
     * @deprecated Use product_NestedLoops instead.
     */
    public static Operator product_ByRun(Operator input,
                                                 RowType leftType,
                                                 RowType rightType)
    {
        return new Product_ByRun(input, leftType, rightType);
    }

    public static Operator product_NestedLoops(Operator outerInput,
                                                       Operator innerInput,
                                                       RowType outerType,
                                                       RowType innerType,
                                                       int inputBindingPosition)
    {
        return new Product_NestedLoops(outerInput, innerInput, outerType, innerType, inputBindingPosition);
    }

    // Count

    public static Operator count_Default(Operator input,
                                                 RowType countType)
    {
        return new Count_Default(input, countType);
    }

    // Sort

    public static Operator sort_InsertionLimited(Operator inputOperator,
                                                         RowType sortType,
                                                         Ordering ordering,
                                                         int limit)
    {
        return new Sort_InsertionLimited(inputOperator, sortType, ordering, limit);
    }

    public static Operator sort_Tree(Operator inputOperator, RowType sortType, Ordering ordering)
    {
        return new Sort_Tree(inputOperator, sortType, ordering);
    }

    public static Ordering ordering()
    {
        return new Ordering();
    }

    // Map

    public static Operator map_NestedLoops(Operator outerInput,
                                                   Operator innerInput,
                                                   int inputBindingPosition)
    {
        return new Map_NestedLoops(outerInput, innerInput, null, null, inputBindingPosition);
    }

    public static Operator map_NestedLoops(Operator outerInput,
                                                   Operator innerInput,
                                                   RowType outerJoinRowType,
                                                   List<Expression> outerJoinRowExpressions,
                                                   int inputBindingPosition)
    {
        return new Map_NestedLoops(outerInput,
                                   innerInput,
                                   outerJoinRowType,
                                   outerJoinRowExpressions,
                                   inputBindingPosition);
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

    public static Cursor cursor(Operator root, StoreAdapter adapter)
    {
        // if all they need is the wrapped cursor, create it directly
        return new TopLevelWrappingCursor(root.cursor(adapter));
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

    public static enum LookupOption {
        KEEP_INPUT,
        DISCARD_INPUT
    }

    // Ordering specification

    public static class Ordering
    {
        public String toString()
        {
            StringBuilder buffer = new StringBuilder();
            for (int i = 0; i < expressions.size(); i++) {
                if (i > 0) {
                    buffer.append(", ");
                }
                buffer.append(expressions.get(i));
                buffer.append(' ');
                buffer.append(directions.get(i) ? "ASC" : "DESC");
            }
            return buffer.toString();
        }

        public int sortFields()
        {
            return evaluations.size();
        }

        public ExpressionEvaluation evaluation(int i)
        {
            return evaluations.get(i);
        }

        public AkType type(int i)
        {
            return expressions.get(i).valueType();
        }

        public boolean ascending(int i)
        {
            return directions.get(i);
        }

        public void append(Expression expression, boolean ascending)
        {
            expressions.add(expression);
            evaluations.add(expression.evaluation());
            directions.add(ascending);
        }

        public Ordering copy()
        {
            Ordering copy = new Ordering();
            copy.expressions.addAll(expressions);
            copy.evaluations.addAll(evaluations);
            copy.directions.addAll(directions);
            return copy;
        }

        private final List<com.akiban.server.expression.Expression> expressions =
            new ArrayList<com.akiban.server.expression.Expression>();
        private final List<ExpressionEvaluation> evaluations = new ArrayList<ExpressionEvaluation>();
        private final List<Boolean> directions = new ArrayList<Boolean>(); // true: ascending, false: descending
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
