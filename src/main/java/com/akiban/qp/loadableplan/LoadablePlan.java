
package com.akiban.qp.loadableplan;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;

import java.util.ArrayList;
import java.util.List;

public abstract class LoadablePlan<T>
{
    public abstract T plan();

    public abstract int[] jdbcTypes();

    public List<String> columnNames()
    {
        List<String> columnNames = new ArrayList<>();
        int columns = jdbcTypes().length;
        for (int c = 0; c < columns; c++) {
            columnNames.add(String.format("c%d", c));
        }
        return columnNames;
    }

    public final void ais(AkibanInformationSchema ais)
    {
        this.ais = ais;
    }

    public final AkibanInformationSchema ais()
    {
        return ais;
    }

    protected Schema schema()
    {
        return SchemaCache.globalSchema(ais);
    }

    private AkibanInformationSchema ais;
}
