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

import java.util.Deque;
import java.util.ArrayDeque;

public class PlanToString implements PlanVisitor, ExpressionVisitor
{
    public static String of(PlanNode plan) {
        PlanToString str = new PlanToString();
        str.add(plan);
        return str.toString();
    }
    
    private StringBuilder string = new StringBuilder();
    private Deque<PlanNode> pending = new ArrayDeque<PlanNode>();
    private int planDepth = 0, expressionDepth = 0;

    protected void add(PlanNode n) {
        pending.addLast(n);
    }

    @Override
    public String toString() {
        while (!pending.isEmpty()) {
            PlanNode p = pending.removeFirst();
            if (string.length() > 0)
                string.append("\n");
            p.accept(this);
        }
        return string.toString();
    }

    @Override
    public boolean visitEnter(PlanNode n) {
        boolean result = visit(n);
        planDepth++;
        return result;
    }

    @Override
    public boolean visitLeave(PlanNode n) {
        planDepth--;
        return true;
    }

    @Override
    public boolean visit(PlanNode n) {
        if (expressionDepth > 0) {
            // Don't print subquery in expression until get back out to top-level.
            add(n);
            return false;
        }
        if (string.length() > 0) string.append("\n");
        for (int i = 0; i < planDepth; i++)
            string.append("  ");
        string.append(n.summaryString());
        return true;
    }
    
    @Override
    public boolean visitEnter(ExpressionNode n) {
        boolean result = visit(n);
        expressionDepth++;
        return result;
    }

    @Override
    public boolean visitLeave(ExpressionNode n) {
        expressionDepth--;
        return true;
    }

    @Override
    public boolean visit(ExpressionNode n) {
        return true;
    }
}
