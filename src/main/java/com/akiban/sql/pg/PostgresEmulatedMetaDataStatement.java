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
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
                         "AND n.nspname <> 'information_schema'\\s+)?" +
                         "AND n.nspname !~ '\\^pg_toast'\\s+" +
                         "(AND c.relname ~ '(.+)'\\s+)?" +
                         "(AND n.nspname ~ '(.+)'\\s+)?" +
                         "(AND pg_catalog.pg_table_is_visible\\(c.oid\\)\\s+)?" +
                         "ORDER BY 1,2;?", true),
        // PSQL \d[S+] NAME
        PSQL_DESCRIBE_TABLES("SELECT c.oid,\\s*" +
                             "n.nspname,\\s*" +
                             "c.relname\\s+" +
                             "FROM pg_catalog.pg_class c\\s+" +
                             "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\\s+" +
                             "WHERE " +
                             "(c.relname ~ '(.+)'\\s+)?" +
                             "((?:AND )?n.nspname ~ '(.+)'\\s+)?" +
                             "(AND pg_catalog.pg_table_is_visible\\(c.oid\\)\\s+)?" +
                             "ORDER BY 2, 3;?", true),

        PSQL_DESCRIBE_TABLES_2("SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relhasoids, '', c.reltablespace\\s+" +
                               "FROM pg_catalog.pg_class c\\s+" +
                               "LEFT JOIN pg_catalog.pg_class tc ON \\(c.reltoastrelid = tc.oid\\)\\s+" +
                               "WHERE c.oid = '(\\d+)';?\\s*", true),
        PSQL_DESCRIBE_TABLES_3("SELECT a.attname,\\s*" +
                               "pg_catalog.format_type\\(a.atttypid, a.atttypmod\\),\\s*" +
                               "\\(SELECT substring\\(pg_catalog.pg_get_expr\\(d.adbin, d.adrelid\\) for 128\\)\\s*" +
                               "FROM pg_catalog.pg_attrdef d\\s+" +
                               "WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef\\),\\s*" +
                               "a.attnotnull, a.attnum,\\s*" +
                               "NULL AS attcollation\\s+" +
                               "FROM pg_catalog.pg_attribute a\\s+" +
                               "WHERE a.attrelid = '(\\d+)' AND a.attnum > 0 AND NOT a.attisdropped\\s+" +
                               "ORDER BY a.attnum;?", true),
        PSQL_DESCRIBE_TABLES_4A("SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid=i.inhparent AND i.inhrelid = '(\\d+)' ORDER BY inhseqno;?", true),
        PSQL_DESCRIBE_TABLES_4B("SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid=i.inhrelid AND i.inhparent = '(\\d+)' ORDER BY c.oid::pg_catalog.regclass::pg_catalog.text;?", true);

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

    static final PostgresType BOOL_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.BOOL_TYPE_OID.getOid(), (short)1, -1, AkType.BOOL, MNumeric.TINYINT.instance());
    static final PostgresType INT2_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.INT2_TYPE_OID.getOid(), (short)2, -1, AkType.LONG, MNumeric.BIGINT.instance());
    static final PostgresType OID_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.OID_TYPE_OID.getOid(), (short)4, -1, AkType.LONG, MNumeric.BIGINT.instance());
    static final PostgresType TYPNAME_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.NAME_TYPE_OID.getOid(), (short)255, -1, AkType.VARCHAR, MString.VARCHAR.instance());
    static final PostgresType IDENT_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.NAME_TYPE_OID.getOid(), (short)128, -1, AkType.VARCHAR, MString.VARCHAR.instance());
    static final PostgresType LIST_TYPE_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.NAME_TYPE_OID.getOid(), (short)13, -1, AkType.VARCHAR, MString.VARCHAR.instance());
    static final PostgresType CHAR0_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.NAME_TYPE_OID.getOid(), (short)0, -1, AkType.VARCHAR, MString.VARCHAR.instance());
    static final PostgresType CHAR1_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.NAME_TYPE_OID.getOid(), (short)1, -1, AkType.VARCHAR, MString.VARCHAR.instance());

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
        case PSQL_DESCRIBE_TABLES:
            ncols = 3;
            names = new String[] { "oid", "nspname", "relname" };
            types = new PostgresType[] { OID_PG_TYPE, IDENT_PG_TYPE, IDENT_PG_TYPE };
            break;
        case PSQL_DESCRIBE_TABLES_2:
            ncols = 8;
            names = new String[] { "relchecks", "relkind",  "relhasindex", "relhasrules", "relhastriggers", "relhasoids", "?column?", "reltablespace" };
            types = new PostgresType[] { INT2_PG_TYPE, CHAR1_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, CHAR0_PG_TYPE, OID_PG_TYPE };
            break;
        case PSQL_DESCRIBE_TABLES_3:
            ncols = 6;
            names = new String[] { "attname", "format_type", "?column?", "attnotnull", "attnum", "attcollation" };
            types = new PostgresType[] { IDENT_PG_TYPE, TYPNAME_PG_TYPE, CHAR0_PG_TYPE, BOOL_PG_TYPE, INT2_PG_TYPE, CHAR0_PG_TYPE };
            break;
        case PSQL_DESCRIBE_TABLES_4A:
        case PSQL_DESCRIBE_TABLES_4B:
            ncols = 1;
            names = new String[] { "oid" };
            types = new PostgresType[] { OID_PG_TYPE };
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
        case PSQL_DESCRIBE_TABLES:
            nrows = psqlDescribeTablesQuery(server, messenger, maxrows, usePVals);
            break;
        case PSQL_DESCRIBE_TABLES_2:
            nrows = psqlDescribeTables2Query(server, messenger, maxrows, usePVals);
            break;
        case PSQL_DESCRIBE_TABLES_3:
            nrows = psqlDescribeTables3Query(server, messenger, maxrows, usePVals);
            break;
        case PSQL_DESCRIBE_TABLES_4A:
        case PSQL_DESCRIBE_TABLES_4B:
            nrows = psqlDescribeTables4Query(server, messenger, maxrows, usePVals);
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
        boolean noIS = (groups.get(2) != null);
        Pattern schemaPattern = null, tablePattern = null;
        if (groups.get(3) != null)
            tablePattern = Pattern.compile(groups.get(4));
        if (groups.get(5) != null)
            schemaPattern = Pattern.compile(groups.get(6));
        Iterator<TableName> iter = names.iterator();
        while (iter.hasNext()) {
            TableName name = iter.next();
            if ((noIS &&
                 name.getSchemaName().equals(TableName.INFORMATION_SCHEMA)) ||
                ((schemaPattern != null) && 
                 !schemaPattern.matcher(name.getSchemaName()).matches()) ||
                ((tablePattern != null) && 
                 !tablePattern.matcher(name.getTableName()).matches()))
                iter.remove();
        }
        Collections.sort(names);
    	for (TableName name : names) {
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

    private int psqlDescribeTablesQuery(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        int nrows = 0;
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        List<TableName> names = new ArrayList<TableName>();
        AkibanInformationSchema ais = server.getAIS();
        names.addAll(ais.getUserTables().keySet());
        if (false)              // TODO: Don't have table ids for views.
        names.addAll(ais.getViews().keySet());
        Pattern schemaPattern = null, tablePattern = null;
        if (groups.get(1) != null)
            tablePattern = Pattern.compile(groups.get(2));
        if (groups.get(3) != null)
            schemaPattern = Pattern.compile(groups.get(4));
        Iterator<TableName> iter = names.iterator();
        while (iter.hasNext()) {
            TableName name = iter.next();
            if (((schemaPattern != null) && 
                 !schemaPattern.matcher(name.getSchemaName()).matches()) ||
                ((tablePattern != null) && 
                 !tablePattern.matcher(name.getTableName()).matches()))
                iter.remove();
        }
        Collections.sort(names);
    	for (TableName name : names) {
            messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
            messenger.writeShort(3); // 3 columns for this query
            int id = ais.getUserTable(name).getTableId();
            writeColumn(messenger, encoder, usePVals, 
                        id, OID_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, 
                        name.getSchemaName(), IDENT_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, 
                        name.getTableName(), IDENT_PG_TYPE);
            messenger.sendMessage();
            nrows++;
            if ((maxrows > 0) && (nrows >= maxrows)) {
                break;
            }
        }
        return nrows;
    }

    private int psqlDescribeTables2Query(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        AkibanInformationSchema ais = server.getAIS();
        Table table = ais.getUserTable(Integer.parseInt(groups.get(1)));
        if (table == null) return 0;
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(8); // 8 columns for this query
        writeColumn(messenger, encoder, usePVals, 
                    0, INT2_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, 
                    "r", CHAR1_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, 
                    false, BOOL_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, 
                    false, BOOL_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, 
                    false, BOOL_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, 
                    false, BOOL_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, 
                    "", CHAR0_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, 
                    0, OID_PG_TYPE);
        messenger.sendMessage();
        return 1;
    }

    private int psqlDescribeTables3Query(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        int nrows = 0;
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        AkibanInformationSchema ais = server.getAIS();
        Table table = ais.getUserTable(Integer.parseInt(groups.get(1)));
        if (table == null) return 0;
        for (Column column : table.getColumns()) {
            messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
            messenger.writeShort(6); // 6 columns for this query
            writeColumn(messenger, encoder, usePVals, 
                        column.getName(), IDENT_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, 
                        column.getTypeDescription(), TYPNAME_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, 
                        null, CHAR0_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, 
                        column.getNullable(), BOOL_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, 
                        column.getPosition(), INT2_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, 
                        null, CHAR0_PG_TYPE);
            messenger.sendMessage();
            nrows++;
            if ((maxrows > 0) && (nrows >= maxrows)) {
                break;
            }
        }
        return nrows;
    }

    private int psqlDescribeTables4Query(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        AkibanInformationSchema ais = server.getAIS();
        Table table = ais.getUserTable(Integer.parseInt(groups.get(1)));
        if (table == null) return 0;
        return 0;
    }

}
