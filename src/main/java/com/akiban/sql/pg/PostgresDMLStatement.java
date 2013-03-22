
package com.akiban.sql.pg;

import java.io.IOException;
import java.util.List;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;

/**
 * An ordinary SQL statement.
 */
public abstract class PostgresDMLStatement extends PostgresBaseStatement
{
    private RowType resultRowType;
    private List<String> columnNames;
    private List<PostgresType> columnTypes;
    private PostgresType[] parameterTypes;
    private boolean usesPValues;

    protected PostgresDMLStatement() {
    }

    protected void init(PostgresType[] parameterTypes, boolean usesPValues) {
        this.parameterTypes = parameterTypes;
        this.usesPValues = usesPValues;
    }

    protected void init(RowType resultsRowType,
                        List<String> columnNames,
                        List<PostgresType> columnTypes,
                        PostgresType[] parameterTypes,
                        boolean usesPValues) {
        this.resultRowType = resultsRowType;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.parameterTypes = parameterTypes;
        this.usesPValues = usesPValues;
    }

    public boolean usesPValues() {
        return usesPValues;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<PostgresType> getColumnTypes() {
        return columnTypes;
    }

    public RowType getResultRowType() {
        return resultRowType;
    }
    @Override
    public PostgresType[] getParameterTypes() {
        return parameterTypes;
    }

    @Override
    public void sendDescription(PostgresQueryContext context, boolean always) 
            throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        List<PostgresType> columnTypes = getColumnTypes();
        if (columnTypes == null) {
            if (!always) return;
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
        }
        else {
            messenger.beginMessage(PostgresMessages.ROW_DESCRIPTION_TYPE.code());
            List<String> columnNames = getColumnNames();
            int ncols = columnTypes.size();
            messenger.writeShort(ncols);
            for (int i = 0; i < ncols; i++) {
                PostgresType type = columnTypes.get(i);
                messenger.writeString(columnNames.get(i)); // attname
                messenger.writeInt(0);    // attrelid
                messenger.writeShort(0);  // attnum
                messenger.writeInt(type.getOid()); // atttypid
                messenger.writeShort(type.getLength()); // attlen
                messenger.writeInt(type.getModifier()); // atttypmod
                messenger.writeShort(context.isColumnBinary(i) ? 1 : 0);
            }
        }
        messenger.sendMessage();
    }
    
    protected PostgresOutputter<Row> getRowOutputter(PostgresQueryContext context) {
        return new PostgresRowOutputter(context, this);
    }
}
