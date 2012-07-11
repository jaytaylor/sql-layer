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

package com.akiban.sql.pg;

import com.akiban.server.types.AkType;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.sql.server.ServerValueEncoder;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.*;

/**
 * Canned handling for fixed SQL text that comes from tools that
 * believe they are talking to a real Postgres database.
 */
public class PostgresEmulatedMetaDataStatement implements PostgresStatement
{
    enum Query {
        // ODBC driver sends this at the start; returning no rows is fine (and normal).
        ODBC_LO_TYPE_QUERY("select oid, typbasetype from pg_type where typname = 'lo'"),
        // SEQUEL 3.33.0 (http://sequel.rubyforge.org/) sends this when opening a new connection
        SEQUEL_B_TYPE_QUERY("select oid, typname from pg_type where typtype = 'b'"),
        // PSQL \d[S+]
        PSQL_LIST_TABLES("SELECT n.nspname as \"Schema\",\\s*" +
                         "c.relname as \"Name\",\\s*" +
                         "CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' END as \"Type\",\\s+" +
                         "pg_catalog.pg_get_userbyid\\(c.relowner\\) as \"Owner\"\\s+" +
                         "FROM pg_catalog.pg_class c\\s+" +
                         "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\\s+" +
                         "WHERE c.relkind IN \\((.+)\\)\\s+" +
                         "(AND n.nspname <> 'pg_catalog'\\s+" +
                         "AND n.nspname <> 'information_schema'\\s+)" +
                         "AND n.nspname !~ '\\^pg_toast'\\s+" +
                         "(AND c.relname ~ '(.+)'\\s+)?" +
                         "AND pg_catalog.pg_table_is_visible\\(c.oid\\)\\s+" +
                         "ORDER BY 1,2;?", true);

        private String sql;
        private Pattern pattern;

        Query(String sql) {
            this.sql = sql;
        }

        Query(String str, boolean regexp) {
            if (regexp) {
                pattern = Pattern.compile(str);
            }
            else {
                sql = str;
            }
        }

        public boolean matches(String sql, List<String> groups) {
            if (pattern == null) {
                if (sql.equalsIgnoreCase(this.sql)) {
                    groups.add(sql);
                    return true;
                }
            }
            else {
                Matcher matcher = pattern.matcher(sql);
                if (matcher.matches()) {
                    for (int i = 0; i <= matcher.groupCount(); i++) {
                        groups.add(matcher.group(i));
                    }
                    return true;
                }
            }
            return false;
        }
    }

    private Query query;
    private List<String> groups;

    protected PostgresEmulatedMetaDataStatement(Query query, List<String> groups) {
        this.query = query;
        this.groups = groups;
        for (int i = 0; i < groups.size(); i++) {
            System.out.println("[" + i + "]: " + groups.get(i));
        }
    }

    static final PostgresType OID_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.OID_TYPE_OID.getOid(), (short)4, -1, AkType.LONG, MNumeric.BIGINT.instance());
    static final PostgresType TYPNAME_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.NAME_TYPE_OID.getOid(), (short)255, -1, AkType.VARCHAR, MString.VARCHAR.instance());
    static final PostgresType IDENT_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.NAME_TYPE_OID.getOid(), (short)128, -1, AkType.VARCHAR, MString.VARCHAR.instance());
    static final PostgresType LIST_TYPE_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.NAME_TYPE_OID.getOid(), (short)13, -1, AkType.VARCHAR, MString.VARCHAR.instance());

    @Override
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context, boolean always) 
            throws IOException {
        int ncols;
        String[] names;
        PostgresType[] types;
        switch (query) {
        case ODBC_LO_TYPE_QUERY:
            ncols = 2;
            names = new String[] { "oid", "typbasetype" };
            types = new PostgresType[] { OID_PG_TYPE, OID_PG_TYPE };
            break;
        case SEQUEL_B_TYPE_QUERY:
            ncols = 2;
            names = new String[] { "oid", "typname" };
            types = new PostgresType[] { OID_PG_TYPE, TYPNAME_PG_TYPE };
            break;
        case PSQL_LIST_TABLES:
            ncols = 4;
            names = new String[] { "Schema", "Name", "Type", "Owner" };
            types = new PostgresType[] { IDENT_PG_TYPE, IDENT_PG_TYPE, LIST_TYPE_PG_TYPE, IDENT_PG_TYPE };
            break;
        default:
            return;
        }

        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        messenger.beginMessage(PostgresMessages.ROW_DESCRIPTION_TYPE.code());
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            PostgresType type = types[i];
            messenger.writeString(names[i]); // attname
            messenger.writeInt(0);    // attrelid
            messenger.writeShort(0);  // attnum
            messenger.writeInt(type.getOid()); // atttypid
            messenger.writeShort(type.getLength()); // attlen
            messenger.writeInt(type.getModifier()); // atttypmod
            messenger.writeShort(0);
        }
        messenger.sendMessage();
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows, boolean usePVals) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        switch (query) {
        case ODBC_LO_TYPE_QUERY:
            nrows = odbcLoTypeQuery(messenger, maxrows);
            break;
        case SEQUEL_B_TYPE_QUERY:
            nrows = sequelBTypeQuery(messenger, maxrows, usePVals);
            break;
        case PSQL_LIST_TABLES:
            nrows = psqlListTablesQuery(server, messenger, maxrows, usePVals);
            break;
        }
        {        
          messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
          messenger.writeString("SELECT " + nrows);
          messenger.sendMessage();
        }
        return nrows;
    }

    private int odbcLoTypeQuery(PostgresMessenger messenger, int maxrows) {
        return 0;
    }

    protected void writeColumn(PostgresMessenger messenger, ServerValueEncoder encoder, boolean usePVals,
                               Object value, PostgresType type) throws IOException {
        ByteArrayOutputStream bytes;
        if (usePVals) {
            bytes = encoder.encodePObject(value, type, false);
        }
        else {
            bytes = encoder.encodeObject(value, type, false);
        }
        if (bytes == null) {
            messenger.writeInt(-1);
        } 
        else {
            messenger.writeInt(bytes.size());
            messenger.writeByteStream(bytes);
        }
    }

    private int sequelBTypeQuery(PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        int nrows = 0;
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
    	for (PostgresType.TypeOid pgtype : PostgresType.TypeOid.values()) {
            if (pgtype.getType() == PostgresType.TypeOid.TypType.BASE) {
                messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
                messenger.writeShort(2); // 2 columns for this query
                writeColumn(messenger, encoder, usePVals, 
                            pgtype.getOid(), OID_PG_TYPE);
                writeColumn(messenger, encoder, usePVals, 
                            pgtype.getName(), TYPNAME_PG_TYPE);
                messenger.sendMessage();
                nrows++;
                if ((maxrows > 0) && (nrows >= maxrows)) {
                    break;
                }
            }
        }
        return nrows;
    }

    private int psqlListTablesQuery(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        int nrows = 0;
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        List<String> types = Arrays.asList(groups.get(1).split(","));
        List<TableName> names = new ArrayList<TableName>();
        AkibanInformationSchema ais = server.getAIS();
        if (types.contains("'r'"))
            names.addAll(ais.getUserTables().keySet());
        if (types.contains("'v'"))
            names.addAll(ais.getViews().keySet());
        Collections.sort(names);
    	for (TableName name : names) {
            if (name.getSchemaName().equals(TableName.INFORMATION_SCHEMA)) continue;
            messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
            messenger.writeShort(4); // 4 columns for this query
            writeColumn(messenger, encoder, usePVals, 
                        name.getSchemaName(), IDENT_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, 
                        name.getTableName(), IDENT_PG_TYPE);
            String type = (ais.getColumnar(name).isView()) ? "view" : "table";
            writeColumn(messenger, encoder, usePVals, 
                        type, LIST_TYPE_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, 
                        null, IDENT_PG_TYPE);
            messenger.sendMessage();
            nrows++;
            if ((maxrows > 0) && (nrows >= maxrows)) {
                break;
            }
        }
        return nrows;
    }

}
