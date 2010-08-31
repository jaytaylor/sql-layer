package com.akiban.cserver.loader;

import java.util.ArrayList;
import java.util.List;

import org.apache.velocity.VelocityContext;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Column;
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
                                     AkibaInformationSchema ais) throws Exception
    {
        super(loader, table);
        // Merged table contains columns of child, and columns of parent that don't participate in the join.
        addColumns(childTask.columns());
        List<Column> parentNonKeyColumns = new ArrayList<Column>(parentTask.columns());
        parentNonKeyColumns.removeAll(parentTask.table().getPrimaryKey().getColumns());
        // State about this task needed by others
        pkColumns(childTask.table().getPrimaryKey().getColumns());
        hKey(columns());
        order(parentTask.order());
        // Join the parent table (e.g. order$parent) and the child table (e.g. item$child) to form the final table,
        // (e.g. item$parent). This is done using the join template which joins (x, y) -> output.
        // Output columns are formed by taking all the columns of x and the columns of y which have no
        // counterpart in x.
        String procedureName = String.format("%s$merge_parent", table.getName()
                .getTableName());
        AISTextGenerator generator = new AISTextGenerator(ais);
        VelocityContext context = new VelocityContext();
        context.put("procedureName", procedureName);
        context.put("xTableName", childTask.artifactTableName());
        context.put("xTableColumns", childTask.columns());
        context.put("xJoinColumns", childTask.fkColumns);
        context.put("yTableName", parentTask.artifactTableName());
        context.put("yTableColumns", parentTask.columns());
        context.put("yJoinColumns", parentTask.pkColumns());
        context.put("yOnlyColumns", parentNonKeyColumns);
        context.put("outputTableName", artifactTableName());
        context.put("advanceXOnMatch", true); // because X is on FK side
        context.put("advanceYOnMatch", false); // because Y is on PK side
        context.put("orderBy", false);
        sql(generator.generate(context, "merge_join.vm"));
    }
}