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

package com.akiban.ais.model.staticgrouping;

import java.util.List;

import com.akiban.ais.model.TableName;

/**
 * <p>A visitor for a {@link Grouping} object.</p>
 *
 * <p>The {@link Grouping#traverse(GroupingVisitor)} method will traverse the grouping, defining each group in terms
 * of the group name and root table, and then recursively visiting each {@linkplain JoinDescription} in the group
 * hierarchy.</p>
 *
 * <p>More specifically {@linkplain Grouping#traverse(GroupingVisitor)} will...
 * <ol>
 *  <li>invoke {@linkplain #start(String)}, passing in the static grouping's header information</li>
 *  <li>then, for each group:
 *   <ol>
 *    <li>invoke {@linkplain #visitGroup(Group, TableName)}</li>
 *    <li>then, for each immediate child of the root table:
 *     <ol>
 *      <li>invoke {@linkplain #visitChild(TableName, List, TableName, List)} to describe a single parent-to-child
 *       relationship (in this case, the root-to-child</li>
 *      <li>if <em>and only if</em> the child table has children of its own:
 *       <ol>
 *        <li>invoke {@linkplain #startVisitingChildren()}</li>
 *        <li>recursively call {@linkplain #visitChild(TableName, List, TableName, List)} for each sub-child (this
 *          method will of course recurse further if the children have children, etc.)</li>
 *        <li>invoke {@linkplain #finishVisitingChildren()}</li>
 *       </ol>
 *      </li>
 *    </ol>
 *    </li>
 *   </ol>
 *  </li>
 *  <li>finally, call {@linkplain #end()} and return the result
 * </ol>
 * </p>
 *
 * <p>Note that {@linkplain #startVisitingChildren()} and {@linkplain #finishVisitingChildren()} will not be called
 * if there are no children; that is, a leaf is treated as if it has no children-list, not as if it has a children-list
 * with zero elements. This is by design, since without it you would need a lookahead if you wanted to take actions
 * depending on whether a given table is a node. For instance, in printing out a canonical text representation of
 * a grouping, we want to surround children with opening and closing parenthesis, but we do not want leafs to have
 * <tt>"()"</tt> appended superfluously.</p>
 * 
 * @param <T> the result of this visitation
 */
public interface GroupingVisitor <T>{
    /**
     * Start visiting a grouping scheme.
     * @param defaultSchema the group's default schema; this is the MySQL equivalent of the <tt>USE</tt> command.
     */
    void start(String defaultSchema);

    /**
     * Start visiting a group.
     * @param group the group's descriptor object
     * @param rootTable the group's sole root table
     */
    void visitGroup(Group group, TableName rootTable);

    /**
     * Finish visiting a group.
     */
    void finishGroup();

    /**
     * <p>Visit a non-root table.</p>
     *
     * <p>This is the visitor's equivalent of <tt>TABLE parentName (parentColumns...)
     * REFERENCES childName (childColumns...)</tt>.
     * @param parentName the parent table; you will have already seen this table in a previous call to
     * {@linkplain #visitGroup(Group,TableName)} or this method
     * @param parentColumns the columns on which the parent is joined (i.e., its primary keys)
     * @param childName the child table
     * @param childColumns the columns on which the child is joined (i.e., its FK columns)
     */
    void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns);

    /**
     * <p>Indicates that we're about to visit a table's children. Specifically, the next call to
     * {@linkplain #visitChild(TableName, List, TableName, List)} will have for a parent the last child we saw in
     * the previous {@linkplain #visitChild(TableName, List, TableName, List)}. This parent table will stay the same
     * until we see subsequent calls to this method or {@linkplain #finishVisitingChildren()}.</p>
     *
     * <p>This method will <em>not</em> be invoked if the last child we visited is a leaf.</p>
     *
     * <p>Visitors have the option of canceling this descent by returning <tt>false</tt> from this. Returning
     * <tt>false</tt> will not cancel the rest of the traversal; it just means we won't visit these children (or
     * any of their decendents).</p>
     * @return whether we should decend into these children.
     */
    boolean startVisitingChildren();

    /**
     * Indicates that we are done visiting a table's children; it's time to go back up a level.
     */
    void finishVisitingChildren();

    /**
     * Indicates that we have finished visiting the grouping definition. {@link Grouping#traverse(GroupingVisitor)}
     * will return to its caller whatever its visitor returns via this method.
     * @return whatever your {@linkplain GroupingVisitor} is parameterized to return; the result of your visit.
     */
    T end();
}
