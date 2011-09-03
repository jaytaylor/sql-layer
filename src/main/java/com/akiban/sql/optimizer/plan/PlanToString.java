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

package com.akiban.sql.optimizer.plan;

import java.util.Stack;

public class PlanToString implements PlanVisitor, ExpressionVisitor
{
    public static String of(PlanNode plan) {
        PlanToString str = new PlanToString();
        str.add(plan);
        return str.toString();
    }

    static class Pending {
        PlanNode plan;
        String name;
        
        Pending(PlanNode plan, String name) {
            this.plan = plan;
            this.name = name;
        }
    }
    
    private StringBuilder string = new StringBuilder();
    private Stack<Pending> pending = new Stack<Pending>();
    private int planDepth = 0, expressionDepth = 0;
    
    public void add(PlanNode plan) {
        add(plan, null);
    }
    public void add(PlanNode plan, String name) {
        pending.add(new Pending(plan, name));
    }

    @Override
    public String toString() {
        while (!pending.empty()) {
            Pending p = pending.pop();
            if (string.length() > 0)
                string.append("\n");
            if (p.name != null) {
                string.append(p.name);
                string.append(": ");
            }
            p.plan.accept(this);
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
        string.append(n);
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
