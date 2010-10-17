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
