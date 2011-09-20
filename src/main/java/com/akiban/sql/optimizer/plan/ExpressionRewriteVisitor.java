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

/** A visitor that can rewrite portions of the expression as it goes. */
public interface ExpressionRewriteVisitor
{
    /** Return a replacement for this node (or the node itself). */
    public ExpressionNode visit(ExpressionNode n);
    /** Return <code>true</code> to visit the children first, <code>false</code> the node first. */
    public boolean visitChildrenFirst(ExpressionNode n);
}
