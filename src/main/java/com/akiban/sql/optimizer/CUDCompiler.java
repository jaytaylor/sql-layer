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
package com.akiban.sql.optimizer;

import static com.akiban.qp.physicaloperator.API.ancestorLookup_Default;
import static com.akiban.qp.physicaloperator.API.groupScan_Default;
import static com.akiban.qp.physicaloperator.API.indexScan_Default;
import static com.akiban.qp.physicaloperator.API.select_HKeyOrdered;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.qp.exec.Plannable;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.expression.Expression;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.sql.optimizer.OperatorCompiler.IndexUsage;
import com.akiban.sql.optimizer.OperatorCompiler.Result;
import com.akiban.sql.optimizer.SimplifiedQuery.ColumnCondition;
import com.akiban.sql.optimizer.SimplifiedQuery.ColumnExpressionToIndex;
import com.akiban.sql.optimizer.SimplifiedQuery.TableNode;
import com.akiban.sql.optimizer.SimplifiedQuery.TableNodeOffsets;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.DeleteNode;
import com.akiban.sql.parser.InsertNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.UpdateNode;

/**
 * The CreateUpdateDelete operator compiler. This removes and refactors the 
 * code from the OperatorCompiler specifically for the Insert, Update, and Delete
 * operations. 
 * @author tjoneslo
 *
 */
public class CUDCompiler {
    private CUDCompiler () {
    }
    
    public static Result compileStatement (OperatorCompiler compiler, DMLStatementNode stmtNode, List<ParameterNode> params) {
        
        SimplifiedTableStatement tableStmt = generateStatement(compiler, stmtNode);
        
        PhysicalOperator resultOper = resultsOperator (compiler, tableStmt);
        
        UserTableRowType targetRowType = compiler.tableRowType(tableStmt.getTargetTable());

        Expression[] expressions = generateExpressions (compiler, tableStmt, targetRowType);
        
        ExpressionRow updateRow = new ExpressionRow(targetRowType, expressions);

        Plannable plan = generatePlan (stmtNode.getNodeType(), resultOper, updateRow);
        return new Result(plan, compiler.getParameterTypes(params));
    }
    

    private static SimplifiedTableStatement generateStatement (OperatorCompiler compiler, DMLStatementNode stmtNode) {
        SimplifiedTableStatement tableStatement;
        compiler.bindAndGroup(stmtNode);
        
        switch (stmtNode.getNodeType()) {
        case NodeTypes.UPDATE_NODE :
            tableStatement = new SimplifiedUpdateStatement ((UpdateNode)stmtNode, compiler.getJoinConditions());
            break;
        case NodeTypes.INSERT_NODE:
            tableStatement = new SimplifiedInsertStatement ((InsertNode)stmtNode, compiler.getJoinConditions());
            break;
        case NodeTypes.DELETE_NODE:
            tableStatement = new SimplifiedDeleteStatement ((DeleteNode)stmtNode, compiler.getJoinConditions());
        default:
            throw new UnsupportedSQLException (stmtNode.statementToString(), stmtNode);        
        }
        tableStatement.reorderJoins();
        return tableStatement;
    }
    
    private static Plannable generatePlan(int nodeType, PhysicalOperator resultOper,
            ExpressionRow updateRow) {
        UpdatePlannable plan = null;
        switch (nodeType) {
        case NodeTypes.UPDATE_NODE:
            plan = new com.akiban.qp.physicaloperator.Update_Default(resultOper,
                    new ExpressionRowUpdateFunction(updateRow));
            break;
        case NodeTypes.INSERT_NODE:
            plan = new com.akiban.qp.physicaloperator.Insert_Default (resultOper,
                    new ExpressionRowUpdateFunction(updateRow));
            break;
        case NodeTypes.DELETE_NODE:
            plan = new com.akiban.qp.physicaloperator.Delete_Default (resultOper,
                    new ExpressionRowUpdateFunction(updateRow));
            break;
        }
        return plan;
    }
    
    private static PhysicalOperator resultsOperator (OperatorCompiler compiler, SimplifiedTableStatement stmt) {
        PhysicalOperator scan; 
        IndexUsage index = compiler.pickBestIndex(stmt);
        UserTableRowType targetRowType = compiler.tableRowType(stmt.getTargetTable());
        GroupTable groupTable = stmt.getTargetTable().getGroupTable();
        if (index != null) {
            assert (stmt.getTargetTable() == index.getLeafMostTable());
            stmt.removeConditions(index.getIndexConditions());
            Index iindex = index.getIndex();
            IndexRowType indexRowType = targetRowType.indexRowType(iindex);
            scan = indexScan_Default(indexRowType, 
                                               index.isReverse(),
                                               index.getIndexKeyRange());
            List<RowType> ancestors = Collections.<RowType>singletonList(targetRowType);
            scan = ancestorLookup_Default(scan, groupTable,
                                                    indexRowType, ancestors, false);
        }
        else {
            scan =  groupScan_Default(groupTable);
        }
        Map<TableNode,Integer> tableOffsets = new HashMap<TableNode,Integer>(1);
        tableOffsets.put(stmt.getTargetTable(), 0);
        ColumnExpressionToIndex fieldOffsets = new TableNodeOffsets(tableOffsets);
        for (ColumnCondition condition : stmt.getConditions()) {
            Expression predicate = condition.generateExpression(fieldOffsets);
            scan = select_HKeyOrdered(scan, targetRowType, predicate);
        }
        return scan;
    }

    private static Expression[] generateExpressions(OperatorCompiler compiler, SimplifiedTableStatement stmt, UserTableRowType targetRowType) {

        Map<TableNode,Integer> tableOffsets = new HashMap<TableNode,Integer>(1);
        tableOffsets.put(stmt.getTargetTable(), 0);
        ColumnExpressionToIndex fieldOffsets = new TableNodeOffsets(tableOffsets);
        
        Expression[] updates = new Expression[targetRowType.nFields()];
        for (SimplifiedTableStatement.TargetColumn targetColumn : 
                 stmt.getTargetColumns()) {
            updates[targetColumn.getColumn().getPosition()] =
                targetColumn.getValue().generateExpression(fieldOffsets);
        }
        
        return updates;
    }

}
