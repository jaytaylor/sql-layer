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

package com.akiban.ais.model;

import java.io.Serializable;
import java.util.HashSet;

/**
 * <p>Standalone object for describing the grouping for an AIS.</p>
 *
 * <p>The AIS represents a directed, [possibly] cyclic graph; the purpose of grouping, really, is to convert
 * this graph to a set of trees such that each node in the graph appears exactly once in the set of trees.</p>
 *
 * <p>We can represent this translation succinctly by defining the root nodes of each tree, and the graph edges
 * that we should keep in creating the table. That representation is this object.</p>
 * @deprecated Use staticgrouping instead
 */
@Deprecated
public class GroupingDefinition implements Serializable
{
    private HashSet<TableName> rootTables;
    private HashSet<String> edges;

    public HashSet<TableName> getRootTables()
    {
        return rootTables;
    }

    public HashSet<String> getEdges()
    {
        return edges;
    }

    void setRootTables(HashSet<TableName> rootTables)
    {
        this.rootTables = rootTables;
    }

    void setEdges(HashSet<String> edges)
    {
        this.edges = edges;
    }
}
