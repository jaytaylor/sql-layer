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
     * The terminology is confusing. Using the COI example: - parentTask is
     * order$parent(cid, oid), ordered by oid - childTask is item$child(iid,
     * oid), ordered by oid - This task creates item$parent(cid, oid, iid),
     * ordered by oid
     */
    public GenerateParentByMergeTask(BulkLoader loader, UserTable table,
                                     GenerateParentTask parentTask, GenerateChildTask childTask,
                                     AkibaInformationSchema ais) throws Exception
    {
        super(loader, table);
        // Merged table contains columns of parent and columns of child that
        // don't participate in the join.
        addColumns(parentTask.columns());
        List<Column> childNonFKColumns = new ArrayList<Column>(childTask
                .columns());
        childNonFKColumns.removeAll(childTask.fkColumns);
        addColumns(childNonFKColumns);
        // State about this task needed by others
        pkColumns(childTask.table().getPrimaryKey().getColumns());
        hKey(columns());
        order(parentTask.order());
        // Join the parent table (e.g. order$parent) and the child table (e.g.
        // item$child) to form the final table,
        // (e.g. item$parent). This is done using the join template which joins
        // (x, y) -> output.
        // Output columns are formed by taking all the columns of x and the
        // columns of y which have no
        // counterpart in x.
        String procedureName = String.format("%s$merge_parent", table.getName()
                .getTableName());
        AISTextGenerator generator = new AISTextGenerator(ais);
        VelocityContext context = new VelocityContext();
        context.put("procedureName", procedureName);
        context.put("xTableName", parentTask.artifactTableName());
        context.put("xTableColumns", parentTask.columns());
        context.put("xJoinColumns", parentTask.pkColumns());
        context.put("yTableName", childTask.artifactTableName());
        context.put("yTableColumns", childTask.columns());
        context.put("yJoinColumns", childTask.fkColumns);
        context.put("yOnlyColumns", childNonFKColumns);
        context.put("outputTableName", artifactTableName());
        context.put("multipleYPerX", true);
        context.put("orderBy", false);
        sql(generator.generate(context, "merge_join.vm"));
    }
}