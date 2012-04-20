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

import com.akiban.ais.model.AkibanInformationSchema;
import org.apache.velocity.VelocityContext;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.AISTextGenerator;

public class GenerateParentByMergeTask extends GenerateParentTask
{
    @Override
    public String type()
    {
        return "GenerateParentByMerge";
    }

    /*
     * The terminology is confusing. Using the COI example:
     * - parentTask is order$parent(cid, oid), ordered by oid
     * - childTask is item$child(iid, oid), ordered by oid
     * - This task creates item$parent(cid, oid, iid), ordered by oid
     */
    public GenerateParentByMergeTask(BulkLoader loader,
                                     UserTable table,
                                     GenerateParentTask parentTask,
                                     GenerateChildTask childTask,
                                     AkibanInformationSchema ais) throws Exception
    {
        super(loader, table);
        // Merged table contains columns of child, and hkey columns of parent that don't participate in the join.
        addColumns(childTask.columns());
        List<Column> hKeyColumnsFromParent = new ArrayList<Column>(parentTask.hKey());
        hKeyColumnsFromParent.removeAll(parentTask.pkColumns());
        addColumns(hKeyColumnsFromParent);
        // PK
        pkColumns(childTask.table().getPrimaryKey().getColumns());
        // order
        Join join = childTask.table.getParentJoin();
        List<Column> orderColumns = new ArrayList<Column>();
        for (Column parentOrderColumn : parentTask.order()) {
            Column childOrderColumn = join.getMatchingChild(parentOrderColumn);
            orderColumns.add(childOrderColumn == null ? parentOrderColumn : childOrderColumn);
        }
        order(orderColumns);
        // Join the parent table (e.g. order$parent) and the child table (e.g. item$child) to form the final table,
        // (e.g. item$parent). This is done using the join template which joins (x, y) -> output.
        // Output columns are formed by taking all the columns of x and the columns of y which have no
        // counterpart in x.
        String procedureName = String.format("%s$merge_parent", table.getName().getTableName());
        AISTextGenerator generator = new AISTextGenerator(ais);
        VelocityContext context = new VelocityContext();
        context.put("procedureName", procedureName);
        context.put("xTableName", childTask.artifactTableName());
        context.put("xTableColumns", childTask.columns());
        context.put("xJoinColumns", childTask.fkColumns);
        context.put("yTableName", parentTask.artifactTableName());
        context.put("yTableColumns", parentTask.columns());
        context.put("yJoinColumns", parentTask.pkColumns());
        context.put("yOnlyColumns", hKeyColumnsFromParent);
        context.put("outputTableName", artifactTableName());
        context.put("outputTableColumns", columns());
        context.put("advanceXOnMatch", true); // because X is on FK side
        context.put("advanceYOnMatch", false); // because Y is on PK side
        context.put("orderBy", false);
        sql(generator.generate(context, "merge_join.vm"));
        loader.tracker().info("%s %s columns: %s", artifactTableName(), type(), columns());
        loader.tracker().info("%s %s hkey: %s", artifactTableName(), type(), hKey());
        loader.tracker().info("%s %s pkColumns: %s", artifactTableName(), type(), pkColumns());
        loader.tracker().info("%s %s order: %s", artifactTableName(), type(), order());
    }
}