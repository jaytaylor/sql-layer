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

package com.akiban.server.loader;

import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

public class GenerateChildTask extends Task
{
    @Override
    public String type()
    {
        return "GenerateChild";
    }

    public GenerateChildTask(BulkLoader loader, UserTable table)
    {
        super(loader, table, "$child");
        // Get the child columns of the join connecting child table to parent,
        // in the same order as the parent's
        // primary key columns.
        this.table = table;
        Join parentJoin = table.getParentJoin();
        for (Column parentPKColumn : parentJoin.getParent().getPrimaryKey().getColumns()) {
            Column childFKColumn = parentJoin.getMatchingChild(parentPKColumn);
            if (childFKColumn == null) {
                throw new BulkLoader.InternalError(parentPKColumn.toString());
            }
            this.fkColumns.add(childFKColumn);
        }
        // The columns of the $child table are joinColumns and PK columns not
        // already included in joinColumns.
        addColumns(this.fkColumns);
        // Order by fk columns
        order(this.fkColumns);
        for (Column pkColumn : table.getPrimaryKey().getColumns()) {
            if (!this.fkColumns.contains(pkColumn)) {
                addColumn(pkColumn);
            }
        }
        sql(String.format(SQL_TEMPLATE, quote(artifactTableName()),
                          commaSeparatedColumnDeclarations(columns()),
                          commaSeparatedColumnNames(columns()),
                          quote(sourceTableName(table.getName())),
                          commaSeparatedColumnNames(this.fkColumns)));
        loader.tracker().info("%s %s columns: %s", artifactTableName(), type(), columns());
        loader.tracker().info("%s %s hkey: %s", artifactTableName(), type(), hKey());
        loader.tracker().info("%s %s fkColumns: %s", artifactTableName(), type(), this.fkColumns);
        loader.tracker().info("%s %s order: %s", artifactTableName(), type(), order());
    }

    private static final String SQL_TEMPLATE = "create table %s(%s) engine = myisam select %s from %s order by %s";

    protected final UserTable table;
    protected final List<Column> fkColumns = new ArrayList<Column>();
}