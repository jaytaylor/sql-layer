
package com.akiban.sql.pg;

import com.akiban.sql.server.ServerValueEncoder;

import java.util.List;
import java.io.IOException;

public abstract class PostgresOutputter<T>
{
    protected PostgresMessenger messenger;
    protected PostgresQueryContext context;
    protected PostgresDMLStatement statement;
    protected List<PostgresType> columnTypes;
    protected int ncols;
    protected ServerValueEncoder encoder;

    public PostgresOutputter(PostgresQueryContext context,
                             PostgresDMLStatement statement) {
        this.context = context;
        this.statement = statement;
        PostgresServerSession server = context.getServer();
        messenger = server.getMessenger();
        columnTypes = statement.getColumnTypes();
        if (columnTypes != null)
            ncols = columnTypes.size();
        encoder = server.getValueEncoder();
    }

    public void beforeData() throws IOException {}

    public void afterData() throws IOException {}

    public abstract void output(T row, boolean usePVals) throws IOException;
}
