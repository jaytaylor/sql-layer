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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.akiban.ais.model.TableName;

/**
 * Describes a join in terms of its child. The parent's columns are mentioned, but not the parent itself. This
 * is equivalent to something along the lines of
 * <tt>TABLE childTable(col_1,col_2...) REFERENCES <i>*</i>(col_a, col_b...)</tt> <br />
 * (Where <tt>*</tt> represents some table, but we don't know which.)
 *
 * <p>{@linkplain JoinDescription}s must have a valid table name and at least one column pairing (that is, "child's
 * column <tt>foo</tt> references parent's column <tt>bar</tt>. As such, {@linkplain #removeJoinColumn(String, String)}
 * will throw an unchecked exception if you attempt to remove the sole column pairing (if you have previously
 * defined multiple column pairings, you can remove extras as you wish), and instances of this class must be
 * instantiated with the first column pairing already defined.</p>
 *
 * <p>If you need a more flexible approach to creating instances of this class, try {@link JoinDescriptionBuilder}.</p>
 */
final class JoinDescription
{
    private final TableName childTable;
    private final List<String> parentColumns = new ArrayList<String>();
    private final List<String> childColumns = new ArrayList<String>();

    public JoinDescription(TableName childTable, List<String> childColumns, List<String> parentColumns)
    {
        if (childTable == null) {
            throw new IllegalArgumentException("child table may not be null");
        }
        this.childTable = childTable;
        replaceJoinColumns(parentColumns, childColumns);
    }
    
    public TableName getChildTableName()
    {
        return childTable;
    }

    /**
     * Returns a copy of this join's parent columns. We do not give you access to the backing list, since
     * this class has invariants based on that list.
     * @return a copy of the parent columns; "[bar]" in <tt>table child(foo) references *(bar)</tt>.
     */
    public List<String> getParentColumns() {
        return new ArrayList<String>(parentColumns);
    }

    /**
     * Returns a copy of this join's child columns. We do not give you access to the backing list, since
     * this class has invariants based on that list.
     * @return a copy of the child columns; "[foo]" in <tt>table child(foo) references *(bar)</tt>.
     */
    public List<String> getChildColumns() {
        return new ArrayList<String>(childColumns);
    }

    private void addJoinColumns(List<String> parents, List<String> children, boolean clearFirst) {
        if (parents.size() != children.size()) {
            throw new IllegalArgumentException("parent and children columns must have same number of items: "
                    + "mismatch between parents " + parents + " and children " + children);
        }

        List<String> parentsToCheck = clearFirst ? Collections.<String>emptyList() : parentColumns;
        List<String> childrenToCheck = clearFirst ? Collections.<String>emptyList() : childColumns;
        checkColumnsValidity("parent", parentsToCheck, parents);
        checkColumnsValidity("parent", childrenToCheck, children);
        if (clearFirst) {
            parentColumns.clear();
            childColumns.clear();
        }
        parentColumns.addAll(parents);
        childColumns.addAll(children);
    }

    /**
     * Adds a new column pairing to this join.
     *
     * <p>For instance, if your join is <tt>table child(foo) references *(bar)</tt> and you pass <tt>("one", "two")</tt>
     * to this method, the join will now be <tt>table child(foo,two) references *(bar,one)</tt>
     * @param parent must not be null, empty or already in the parent-columns list
     * @param child must not be null, empty or already in the child-columns list
     */
    public void addJoinColumn(String parent, String child)
    {
        checkColumnValidity("parent", parentColumns, parent);
        checkColumnValidity("child", childColumns, child);

        parentColumns.add(parent);
        childColumns.add(child);
    }

    public void replaceJoinColumns(List<String> parentColumns, List<String> childColumns) {
        if (childColumns.size() == 0) {
            // addJoinColumns will test that the list sizes are the same,
            // so we don't need to check parentColumns.size() == 0
            throw new IllegalArgumentException("can't initialize JoinDescription with empty list");
        }
        addJoinColumns(parentColumns, childColumns, true);
    }

    /**
     * Checks that a column is valid; that it's non-empty and non-null
     * @param name for error reporting
     * @param value the string to check
     */
    static void checkColumnValidity(String name, String value) {
        if (value == null) {
            throw new IllegalArgumentException(name + " column may not be null");
        }
        if (value.length() == 0) {
            throw new IllegalArgumentException(name + " column may not be empty");
        }
    }

    /**
     * Checks that the given column is valid: that it's non-null, non-empty and not already in the given List of
     * Strings.
     * @param name for error reporting
     * @param existingColumns the list of strings that we can't have duplicates for
     * @param value the new string we'd like to add to existingColumns
     */
    private static void checkColumnValidity(String name, List<String> existingColumns, String value) {
        checkColumnValidity(name, value);
        if (existingColumns.contains(value)) {
            throw new IllegalArgumentException(name + " column already in join: " + value);
        }
    }

    /**
     * Checks that all of the given columns are valid: that they're non-null, non-empty strings and not already
     * in the given List of Strings.
     * @param name for error reporting
     * @param existingColumns the list of strings that we can't have duplicates for
     * @param newColumns the new strings we'd like to add to existingColumns
     */
    private static void checkColumnsValidity(String name, List<String> existingColumns, List<String> newColumns) {
        int i = 0;
        for (String column : newColumns) {
            checkColumnValidity(name + '['+(i++)+']', existingColumns, column);
        }
    }

    /**
     * Removes a join-column pairing.
     *
     * <p>Both given column names must be present in their respective lists, and they must have the same index. Calls
     * to this method may not result in a {@linkplain JoinDescription} with no column pairings; trying to do so
     * will result in an unchecked exception being thrown.</p>
     *
     * <p>For instance, if your join is <tt>table child(col_1, col_A) references *(c1,cA)</tt> and you pass
     * in <tt>("col_1", "c1")</tt>, the join will now be <tt>table child(col_A) references *(cA)</tt>.</p>
     * @param parent a column in the child-joins list
     * @param child a column in the parent-joins list
     */
    @SuppressWarnings("unused")
    public void removeJoinColumn(String parent, String child)
    {
        int parentIndex = parentColumns.indexOf(parent);
        int childIndex = childColumns.indexOf(child);

        if (parentIndex < 0) {
            throw new IllegalArgumentException("parent column not in join: " + parent);
        }
        if (childIndex < 0) {
            throw new IllegalArgumentException("child column not in join: " + child);
        }
        if (parentIndex != childIndex) {
            String msg = "mismatched column indexes; tried to remove (" + parent + ',' + child
                    + ") but column lists were (" + parentColumns + ", " + childColumns + ')';
            throw new IllegalArgumentException(msg);
        }

        if (parentColumns.size() == 1) {
            throw new IllegalStateException("may not remove last column pairing: "
                    + "<" + child + "> references <" + parent + ">");
        }

        parentColumns.remove(parent);
        childColumns.remove(child);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        JoinDescription that = (JoinDescription) o;
        return childTable.equals(that.childTable);
    }

    @Override
    public int hashCode()
    {
        return childTable.hashCode();
    }

    @Override
    public String toString()
    {
        return JoinDescription.class.getName()
                + '[' + childTable + childColumns + " references parent's " + parentColumns + ']';
    }
}
