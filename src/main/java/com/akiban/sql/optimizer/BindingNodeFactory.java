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

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;

import java.util.*;

/**
 * A <code>NodeFactory</code> that knows how to properly clone bindings.
 */
public class BindingNodeFactory extends NodeFactory
{
    private NodeFactory inner;

    private Map<QueryTreeNode,QueryTreeNode> nodeMap;
    private Collection<QueryTreeNode> bindingsToCopy;

    public static void wrap(SQLParser parser) {
        NodeFactory nodeFactory = parser.getNodeFactory();
        if (!(nodeFactory instanceof BindingNodeFactory))
            parser.setNodeFactory(new BindingNodeFactory(nodeFactory));
    }

    /** Construct BindingNodeFactory encapsulating original. */
    public BindingNodeFactory(NodeFactory inner) {
        this.inner = inner;
    }

    /** Create node: just passes along to encapsulated. */
    public QueryTreeNode getNode(int nodeType, SQLParserContext pc)
            throws StandardException {
        return inner.getNode(nodeType, pc);
    }

    /** Copy node: pass along, but keep track mappings for those kinds
     * of nodes that are used in bindings. */
    public QueryTreeNode copyNode(QueryTreeNode node, SQLParserContext pc)
            throws StandardException {
        boolean newMap = false;
        if (nodeMap == null) {
            nodeMap = new HashMap<QueryTreeNode,QueryTreeNode>();
            bindingsToCopy = new ArrayList<QueryTreeNode>();
            newMap = true;
        }
        try {
            QueryTreeNode result = inner.copyNode(node, pc);
            if ((node instanceof FromTable) ||
                (node instanceof ResultColumn))
                nodeMap.put(node, result);
            return result;
        }
        finally {
            if (newMap) {
                copyBindings();
                nodeMap = null;
                bindingsToCopy = null;
            }
        }
    }

    /** Copy user data, which is known to be a appropriate binding object. */
    public Object copyUserData(QueryTreeNode node, Object userData)
            throws StandardException {
        if ((userData instanceof TableBinding) ||
            (userData instanceof ColumnBinding)) {
            // Remember to fix later after all copies done.
            bindingsToCopy.add(node);
        }
        // Just return same data for now.
        return userData;
    }
    
    /** Do the actual binding copies.
     * Deferred like this so that it does not matter what order the tree
     * is traversed during the node copy.
     */
    protected void copyBindings() throws StandardException {
        for (QueryTreeNode node : bindingsToCopy) {
            Object userData = node.getUserData();
            if (userData instanceof TableBinding)
                userData = new TableBinding((TableBinding)userData);
            else if (userData instanceof ColumnBinding) {
                ColumnBinding cb = (ColumnBinding)userData;
                FromTable oldFromTable = cb.getFromTable();
                FromTable newFromTable = (FromTable)nodeMap.get(oldFromTable);
                if (newFromTable == null)
                    newFromTable = oldFromTable; // Not cloned in this subtree.
                ResultColumn oldResultColumn = cb.getResultColumn();
                if (oldResultColumn == null) {
                    userData = new ColumnBinding(newFromTable,
                                                 cb.getColumn(), cb.isNullable());
                }
                else {
                    ResultColumn newResultColumn = (ResultColumn)nodeMap.get(oldResultColumn);
                    if (newResultColumn == null)
                        newResultColumn = oldResultColumn;
                    userData = new ColumnBinding(newFromTable, newResultColumn);
                }
            }
            else {
                continue;
            }
            node.setUserData(userData);
        }
    }

}
