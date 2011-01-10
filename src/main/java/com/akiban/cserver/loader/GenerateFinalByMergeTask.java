package com.akiban.cserver.loader;

import java.util.ArrayList;
import java.util.List;

import org.apache.velocity.VelocityContext;

import com.akiban.ais.model.AkibaInformationSchema;
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
                                    AkibaInformationSchema ais)
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