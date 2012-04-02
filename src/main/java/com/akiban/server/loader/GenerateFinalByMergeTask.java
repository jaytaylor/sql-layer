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

import org.apache.velocity.VelocityContext;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.AISTextGenerator;

public class GenerateFinalByMergeTask extends GenerateFinalTask
{
    @Override
    public String type()
    {
        return "GenerateFinalByMerge";
    }

    /*
     * We are computing T$final for table T by doing a merge of x and y where:
      *  E.g., for the COI example, computing I:
     * - x: I(iid, oid, ...)
     * - y: I$parent(cid, oid, iid)
     * - result: I$final(iid, oid, ..., cid)
     * 
     * In I$parent, cid and oid are the Column objects from the O table. But in
     * I, oid is of course from the I table. So in computing the columns for
     * I$final, we need to map the O.oid column to I.oid to recognize the fact
     * that I$final already has an oid column.
     */

    public GenerateFinalByMergeTask(BulkLoader loader,
                                    UserTable table,
                                    GenerateParentTask parentTask,
                                    AkibanInformationSchema ais)
        throws Exception
    {
        super(loader, table);
        // Final table contains columns of original table and other columns from
        // parentTask that complete the hkey.
        addColumns(table.getColumns());
        List<Column> hKeyColumnsNotInOriginalTable = new ArrayList<Column>();
        for (Column hKeyColumn : table.allHKeyColumns()) {
            if (hKeyColumn.getTable() != table) {
                hKeyColumnsNotInOriginalTable.add(hKeyColumn);
            }
        }
        addColumns(hKeyColumnsNotInOriginalTable);
        order(hKey());
        // Compute original table's column positions. Original table columns were added to columns,
        // so it's just the first elements of columns. But verify just to be safe.
        columnPositions = new int[table.getColumns().size()];
        int p = 0;
        for (Column column : table.getColumns()) {
            int columnPosition = columns().indexOf(column);
            if (columnPosition == -1) {
                throw new BulkLoader.InternalError(column.toString());
            }
            columnPositions[p++] = columnPosition;
        }
        // Join the original table (e.g. item) and the parent table (e.g. item$parent) to form the final table,
        // (e.g. item$final). This is done using the join template which joins (x, y) -> output. Output columns are
        // formed by taking all the columns of x and the columns of y which have no counterpart in x.
        String procedureName = String.format("%s$merge_final", table.getName().getTableName());
        AISTextGenerator generator = new AISTextGenerator(ais);
        VelocityContext context = new VelocityContext();
        context.put("procedureName", procedureName);
        context.put("xTableName", sourceTableName(table.getName()));
        context.put("xTableColumns", table.getColumns());
        context.put("xJoinColumns", table.getPrimaryKey().getColumns());
        context.put("yTableName", parentTask.artifactTableName());
        context.put("yTableColumns", parentTask.columns());
        context.put("yJoinColumns", parentTask.pkColumns());
        context.put("yOnlyColumns", hKeyColumnsNotInOriginalTable);
        context.put("outputTableName", artifactTableName());
        context.put("outputTableColumns", columns());
        context.put("advanceXOnMatch", true); // because X is a PK
        context.put("advanceYOnMatch", true); // because Y is a PK too
        context.put("orderBy", true);
        sql(generator.generate(context, "merge_join.vm"));
        loader.tracker().info("%s %s columns: %s", artifactTableName(), type(), columns());
        loader.tracker().info("%s %s hkey: %s", artifactTableName(), type(), hKey());
        loader.tracker().info("%s %s order: %s", artifactTableName(), type(), order());
        loader.tracker().info("%s %s columnPositions: %s",
                              artifactTableName(), type(), toString(columnPositions));
        loader.tracker().info("%s %s hKeyColumnPositions: %s",
                              artifactTableName(), type(), toString(hKeyColumnPositions()));
    }
}