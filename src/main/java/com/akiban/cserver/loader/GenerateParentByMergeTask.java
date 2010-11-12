package com.akiban.cserver.loader;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.AISTextGenerator;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.List;

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