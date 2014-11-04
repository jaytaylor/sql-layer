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

package com.foundationdb.sql.optimizer;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.Visitable;
import com.foundationdb.sql.parser.Visitor;
import com.foundationdb.server.error.SQLParserInternalException;

import java.util.ArrayList;
import java.util.List;

public class ParameterFinder implements Visitor
{
    private List<ParameterNode> parameters;

    public ParameterFinder() {
    }

    public List<ParameterNode> find(Visitable root) {
        parameters = new ArrayList<>();
        try {
            root.accept(this);
        } 
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
        return parameters;
    }

    @Override
    public Visitable visit(Visitable node) {
        if (node instanceof ParameterNode)
            parameters.add((ParameterNode)node);
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return false;
    }
    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }
    @Override
    public boolean stopTraversal() {
        return false;
    }

}
