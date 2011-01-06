package com.akiban.ais.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PrimaryKey implements Serializable
{
    public List<Column> getColumns()
    {
        return columns;
    }

    public Index getIndex()
    {
        return index;
    }

    public Boolean getRealPrimaryKey()
    {
        // A not-real primary key is one created by us for a table declared without a PK. We need a PK
        // so that rows of the table can have hkey values. A not-real primary key has a single column
        // that is not real.
        return !(columns.size() == 1 && !columns.get(0).getRealColumn());
    }

    public PrimaryKey()
    {
        // GWT: needs default constructor
    }

    public PrimaryKey(Index index)
    {
        this.index = index;
        List<Column> columns = new ArrayList<Column>();
        for (IndexColumn indexColumn : index.getColumns()) {
            columns.add(indexColumn.getColumn());
        }
        this.columns = columns;
    }

    private Index index;
    private List<Column> columns;
}
