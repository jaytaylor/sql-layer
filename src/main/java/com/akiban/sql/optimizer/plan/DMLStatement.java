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
package com.akiban.sql.optimizer.plan;

import java.util.List;

import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.rule.EquivalenceFinder;

public class DMLStatement extends BaseStatement {

    public DMLStatement(PlanNode input, BaseUpdateStatement.StatementType type, 
                        TableSource selectTable,
                        TableNode targetTable,
                        List<ResultField> results,
                        TableSource returningTable,
                        EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(input, columnEquivalencies);
        this.type = type;
        this.selectTable = selectTable;
        this.targetTable = targetTable;
        this.results = results;
        this.returningTable = returningTable;
    }
    
    public BaseUpdateStatement.StatementType getType() {
        return type;
    }
    
    public List<ResultField> getResultField() {
        return results;
    }

    public TableSource getSelectTable() { 
        return selectTable;
    }
    public TableSource getReturningTable() { 
        return returningTable;
    }

    public TableNode getTargetTable() {
        return targetTable;
    }
    
    public boolean isRequireStepIsolation() {
        return requireStepIsolation;
    }
    public void setRequireStepIsolation(boolean requireStepIsolation) {
        this.requireStepIsolation = requireStepIsolation;
    }
    
    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append('(');
        str.append(targetTable);
        if (requireStepIsolation)
            str.append(", HALLOWEEN");
        str.append(')');
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
    }
    
    private final BaseUpdateStatement.StatementType type; 
    private final List<ResultField> results;
    private final TableSource selectTable, returningTable;
    private TableNode targetTable;
    private boolean requireStepIsolation;

}
