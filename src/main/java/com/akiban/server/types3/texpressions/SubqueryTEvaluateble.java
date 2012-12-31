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

package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

abstract class SubqueryTEvaluateble implements TEvaluatableExpression {

    @Override
    public PValueSource resultValue() {
        return pvalue;
    }

    @Override
    public void evaluate() {
        context.setRow(bindingPosition, outerRow);
        cursor.open();
        try {
            doEval(pvalue);
        }
        finally {
            cursor.close();
        }
    }

    @Override
    public void with(Row row) {
        if (row.rowType() != outerRowType) {
            throw new IllegalArgumentException("wrong row type: " + outerRowType +
                    " != " + row.rowType());
        }
        outerRow = row;
    }

    @Override
    public void with(QueryContext context) {
        this.context = context;
        this.cursor = API.cursor(subquery, context);
    }

    protected abstract void doEval(PValueTarget out);

    protected QueryContext queryContext() {
        return context;
    }

    protected Row next() {
        Row row = cursor.next();
        if ((row != null) &&
                (row.rowType() != innerRowType)) {
            throw new IllegalArgumentException("wrong row type: " + innerRowType +
                    " != " + row.rowType());
        }
        return row;
    }

    SubqueryTEvaluateble(Operator subquery, RowType outerRowType, RowType innerRowType, int bindingPosition,
                         TInstance underlying)
    {
        this.subquery = subquery;
        this.outerRowType = outerRowType;
        this.innerRowType = innerRowType;
        this.bindingPosition = bindingPosition;
        this.pvalue = new PValue(underlying);
    }

    private final Operator subquery;
    private final RowType outerRowType;
    private final RowType innerRowType;
    private final int bindingPosition;
    private final PValue pvalue;
    private Cursor cursor;
    private QueryContext context;
    private Row outerRow;
}
