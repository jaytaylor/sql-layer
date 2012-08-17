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
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.sql.server.ServerValueEncoder;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.*;
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
        // PSQL \dn
        PSQL_LIST_SCHEMAS("SELECT n.nspname AS \"Name\",\\s*" +
                          "(?:pg_catalog.pg_get_userbyid\\(n.nspowner\\)|u.usename) AS \"Owner\"\\s+" +
                          "FROM pg_catalog.pg_namespace n\\s+" +
                          "(?:LEFT JOIN pg_catalog.pg_user u\\s+" +
                          "ON n.nspowner=u.usesysid\\s+)?" +
                          "(?:WHERE\\s+)?" +
                          "(?:\\(n.nspname !~ '\\^pg_temp_' OR\\s+" + 
                          "n.nspname = \\(pg_catalog.current_schemas\\(true\\)\\)\\[1\\]\\)\\s+)?" +
                          "(n.nspname !~ '\\^pg_' AND n.nspname <> 'information_schema'\\s+)?" + // 1
                          "(?:AND\\s+)?" + 
                          "(n.nspname ~ '(.+)'\\s+)?" + // 2 (3)
                          "ORDER BY 1;?", true),
        // PSQL \d, \dt, \dv
        PSQL_LIST_TABLES("SELECT n.nspname as \"Schema\",\\s*" +
                         "c.relname as \"Name\",\\s*" +
                         "CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' (?:WHEN 'f' THEN 'foreign table' )?END as \"Type\",\\s+" +
                         "(?:pg_catalog.pg_get_userbyid\\(c.relowner\\)|u.usename) as \"Owner\"\\s+" +
                         "FROM pg_catalog.pg_class c\\s+" +
                         "(?:LEFT JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner\\s+)?" +
                         "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\\s+" +
                         "WHERE c.relkind IN \\((.+)\\)\\s+" + // 1
                         "(AND n.nspname <> 'pg_catalog'\\s+" +
                         "AND n.nspname <> 'information_schema'\\s+)?" + // 2
                         "(?:AND n.nspname !~ '\\^pg_toast'\\s+)?" +
                         "(?:(AND n.nspname NOT IN \\('pg_catalog', 'pg_toast'\\)\\s+)|" + // 3
                         "(AND n.nspname = 'pg_catalog')\\s+)?" + // 4
                         "(AND c.relname ~ '(.+)'\\s+)?" + // 5 (6)
                         "(AND n.nspname ~ '(.+)'\\s+)?" + // 7 (8)
                         "(?:AND pg_catalog.pg_table_is_visible\\(c.oid\\)\\s+)?" +
                         "(AND c.relname ~ '(.+)'\\s+)?" + // 9 (10)
                         "ORDER BY 1,2;?", true),
        // PSQL \d NAME
        PSQL_DESCRIBE_TABLES_1("SELECT c.oid,\\s*" +
                               "n.nspname,\\s*" +
                               "c.relname\\s+" +
                               "FROM pg_catalog.pg_class c\\s+" +
                               "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\\s+" +
                               "WHERE " +
                               "(?:pg_catalog.pg_table_is_visible\\(c.oid\\)\\s+AND )?" +
                               "(n.nspname ~ '(.+)'\\s+)?" + // 1 (2)
                               "((?:AND )?c.relname ~ '(.+)'\\s+)?" + // 3 (4)
                               "((?:AND )?n.nspname ~ '(.+)'\\s+)?" + // 5 (6)
                               "(?:AND pg_catalog.pg_table_is_visible\\(c.oid\\)\\s+)?" +
                               "ORDER BY 2, 3;?", true),
        PSQL_DESCRIBE_TABLES_2("SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relhasoids, '', c.reltablespace\\s+" +
                               "FROM pg_catalog.pg_class c\\s+" +
                               "LEFT JOIN pg_catalog.pg_class tc ON \\(c.reltoastrelid = tc.oid\\)\\s+" +
                               "WHERE c.oid = '(-?\\d+)';?\\s*", true), // 1
        PSQL_DESCRIBE_TABLES_2X("SELECT relhasindex, relkind, relchecks, reltriggers, relhasrules\\s+" +
                               "FROM pg_catalog.pg_class\\s+" +
                               "WHERE oid = '(-?\\d+)';?\\s*", true), // 1
        PSQL_DESCRIBE_TABLES_3("SELECT a.attname,\\s*" +
                               "pg_catalog.format_type\\(a.atttypid, a.atttypmod\\),\\s*" +
                               "\\(SELECT substring\\((?:pg_catalog.pg_get_expr\\(d.adbin, d.adrelid\\)|d.adsrc) for 128\\)\\s*" +
                               "FROM pg_catalog.pg_attrdef d\\s+" +
                               "WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef\\),\\s*" +
                               "a.attnotnull, a.attnum,?\\s*" +
                               "(NULL AS attcollation\\s+)?" + // 1
                               "FROM pg_catalog.pg_attribute a\\s+" +
                               "WHERE a.attrelid = '(-?\\d+)' AND a.attnum > 0 AND NOT a.attisdropped\\s+" + // 2
                               "ORDER BY a.attnum;?", true),
        PSQL_DESCRIBE_TABLES_4A("SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid=i.inhparent AND i.inhrelid = '(-?\\d+)' ORDER BY inhseqno;?", true),
        PSQL_DESCRIBE_TABLES_4B("SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid=i.inhrelid AND i.inhparent = '(-?\\d+)' ORDER BY c.oid::pg_catalog.regclass::pg_catalog.text;?", true),
        PSQL_DESCRIBE_TABLES_5("SELECT c.relname FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid=i.inhparent AND i.inhrelid = '(-?\\d+)' ORDER BY inhseqno ASC;?", true),
        PSQL_DESCRIBE_INDEXES("SELECT c2.relname, i.indisprimary, i.indisunique(, i.indisclustered, i.indisvalid)?, pg_catalog.pg_get_indexdef\\(i.indexrelid(?:, 0, true)?\\),?\\s*" + // 1
                              "(null AS constraintdef, null AS contype, false AS condeferrable, false AS condeferred, )?(?:c2.reltablespace)?\\s+" + // 2
                              "FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i\\s+" +
                              "WHERE c.oid = '(-?\\d+)' AND c.oid = i.indrelid AND i.indexrelid = c2.oid\\s+" + // 3
                              "ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;?", true),
        PSQL_DESCRIBE_VIEW("SELECT pg_catalog.pg_get_viewdef\\('(-?\\d+)'::pg_catalog.oid, true\\);?", true);

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
    private boolean usePVals;

    protected PostgresEmulatedMetaDataStatement(Query query, List<String> groups, boolean usePVals) {
        this.query = query;
        this.groups = groups;
        this.usePVals = usePVals;
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
    static final PostgresType INDEXDEF_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.NAME_TYPE_OID.getOid(), (short)1024, -1, AkType.VARCHAR, MString.VARCHAR.instance());
    static final PostgresType VIEWDEF_PG_TYPE = 
        new PostgresType(PostgresType.TypeOid.NAME_TYPE_OID.getOid(), (short)32768, -1, AkType.VARCHAR, MString.VARCHAR.instance());

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
        case PSQL_LIST_SCHEMAS:
            ncols = 2;
            names = new String[] { "Name", "Owner" };
            types = new PostgresType[] { IDENT_PG_TYPE, IDENT_PG_TYPE };
            break;
        case PSQL_LIST_TABLES:
            ncols = 4;
            names = new String[] { "Schema", "Name", "Type", "Owner" };
            types = new PostgresType[] { IDENT_PG_TYPE, IDENT_PG_TYPE, LIST_TYPE_PG_TYPE, IDENT_PG_TYPE };
            break;
        case PSQL_DESCRIBE_TABLES_1:
            ncols = 3;
            names = new String[] { "oid", "nspname", "relname" };
            types = new PostgresType[] { OID_PG_TYPE, IDENT_PG_TYPE, IDENT_PG_TYPE };
            break;
        case PSQL_DESCRIBE_TABLES_2:
            ncols = 8;
            names = new String[] { "relchecks", "relkind",  "relhasindex", "relhasrules", "relhastriggers", "relhasoids", "?column?", "reltablespace" };
            types = new PostgresType[] { INT2_PG_TYPE, CHAR1_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, CHAR0_PG_TYPE, OID_PG_TYPE };
            break;
        case PSQL_DESCRIBE_TABLES_2X:
            ncols = 5;
            names = new String[] { "relhasindex", "relkind", "relchecks", "reltriggers", "relhasrules" };
            types = new PostgresType[] { BOOL_PG_TYPE, CHAR1_PG_TYPE, INT2_PG_TYPE, INT2_PG_TYPE, BOOL_PG_TYPE };
            break;
        case PSQL_DESCRIBE_TABLES_3:
            ncols = (groups.get(1) != null) ? 6 : 5;
            names = new String[] { "attname", "format_type", "?column?", "attnotnull", "attnum", "attcollation" };
            types = new PostgresType[] { IDENT_PG_TYPE, TYPNAME_PG_TYPE, CHAR0_PG_TYPE, BOOL_PG_TYPE, INT2_PG_TYPE, CHAR0_PG_TYPE };
            break;
        case PSQL_DESCRIBE_TABLES_4A:
        case PSQL_DESCRIBE_TABLES_4B:
            ncols = 1;
            names = new String[] { "oid" };
            types = new PostgresType[] { OID_PG_TYPE };
            break;
        case PSQL_DESCRIBE_TABLES_5:
            ncols = 1;
            names = new String[] { "relname" };
            types = new PostgresType[] { IDENT_PG_TYPE };
            break;
        case PSQL_DESCRIBE_INDEXES:
            if (groups.get(1) == null) {
                ncols = 4;
                names = new String[] { "relname", "indisprimary", "indisunique", "pg_get_indexdef" };
                types = new PostgresType[] { IDENT_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, INDEXDEF_PG_TYPE };
            }
            if (groups.get(2) == null) {
                ncols = 7;
                names = new String[] { "relname", "indisprimary", "indisunique", "indisclustered", "indisvalid", "pg_get_indexdef", "reltablespace" };
                types = new PostgresType[] { IDENT_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, INDEXDEF_PG_TYPE, INT2_PG_TYPE };
            }
            else {
                ncols = 11;
                names = new String[] { "relname", "indisprimary", "indisunique", "indisclustered", "indisvalid", "pg_get_indexdef", "constraintdef", "contype", "condeferrable", "condeferred", "reltablespace" };
                types = new PostgresType[] { IDENT_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, INDEXDEF_PG_TYPE, CHAR0_PG_TYPE, CHAR0_PG_TYPE, BOOL_PG_TYPE, BOOL_PG_TYPE, INT2_PG_TYPE };
            }
            break;
        case PSQL_DESCRIBE_VIEW:
            ncols = 1;
            names = new String[] { "pg_get_viewdef" };
            types = new PostgresType[] { VIEWDEF_PG_TYPE };
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
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
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
        case PSQL_LIST_SCHEMAS:
            nrows = psqlListSchemasQuery(server, messenger, maxrows, usePVals);
            break;
        case PSQL_LIST_TABLES:
            nrows = psqlListTablesQuery(server, messenger, maxrows, usePVals);
            break;
        case PSQL_DESCRIBE_TABLES_1:
            nrows = psqlDescribeTables1Query(server, messenger, maxrows, usePVals);
            break;
        case PSQL_DESCRIBE_TABLES_2:
            nrows = psqlDescribeTables2Query(server, messenger, maxrows, usePVals);
            break;
        case PSQL_DESCRIBE_TABLES_2X:
            nrows = psqlDescribeTables2XQuery(server, messenger, maxrows, usePVals);
            break;
        case PSQL_DESCRIBE_TABLES_3:
            nrows = psqlDescribeTables3Query(server, messenger, maxrows, usePVals);
            break;
        case PSQL_DESCRIBE_TABLES_4A:
        case PSQL_DESCRIBE_TABLES_4B:
        case PSQL_DESCRIBE_TABLES_5:
            nrows = psqlDescribeTables4Query(server, messenger, maxrows, usePVals);
            break;
        case PSQL_DESCRIBE_INDEXES:
            nrows = psqlDescribeIndexesQuery(server, messenger, maxrows, usePVals);
            break;
        case PSQL_DESCRIBE_VIEW:
            nrows = psqlDescribeViewQuery(server, messenger, maxrows, usePVals);
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

    private int psqlListSchemasQuery(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        int nrows = 0;
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        AkibanInformationSchema ais = server.getAIS();
        List<String> names = new ArrayList<String>(ais.getSchemas().keySet());
        boolean noIS = (groups.get(1) != null);
        Pattern pattern = null;
        if (groups.get(2) != null)
            pattern = Pattern.compile(groups.get(3));
        Iterator<String> iter = names.iterator();
        while (iter.hasNext()) {
            String name = iter.next();
            if ((noIS &&
                 name.equals(TableName.INFORMATION_SCHEMA)) ||
                ((pattern != null) && 
                 !pattern.matcher(name).find()))
                iter.remove();
        }
        Collections.sort(names);
    	for (String name : names) {
            messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
            messenger.writeShort(2); // 2 columns for this query
            writeColumn(messenger, encoder, usePVals, 
                        name, IDENT_PG_TYPE);
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
        boolean noIS = (groups.get(2) != null) || (groups.get(3) != null);
        boolean onlyIS = (groups.get(4) != null);
        Pattern schemaPattern = null, tablePattern = null;
        if (groups.get(5) != null)
            tablePattern = Pattern.compile(groups.get(6));
        if (groups.get(7) != null)
            schemaPattern = Pattern.compile(groups.get(8));
        if (groups.get(9) != null)
            tablePattern = Pattern.compile(groups.get(10));
        Iterator<TableName> iter = names.iterator();
        while (iter.hasNext()) {
            TableName name = iter.next();
            boolean keep = true;
            if ((name.getSchemaName().equals(TableName.INFORMATION_SCHEMA) ? noIS : onlyIS) ||
                ((schemaPattern != null) && 
                 !schemaPattern.matcher(name.getSchemaName()).find()) ||
                ((tablePattern != null) && 
                 !tablePattern.matcher(name.getTableName()).find()))
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

    private int psqlDescribeTables1Query(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        int nrows = 0;
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        Map<Integer,TableName> nonTableNames = null;
        List<TableName> names = new ArrayList<TableName>();
        AkibanInformationSchema ais = server.getAIS();
        names.addAll(ais.getUserTables().keySet());
        names.addAll(ais.getViews().keySet());
        Pattern schemaPattern = null, tablePattern = null;
        if (groups.get(1) != null)
            schemaPattern = Pattern.compile(groups.get(2));
        if (groups.get(3) != null)
            tablePattern = Pattern.compile(groups.get(4));
        if (groups.get(5) != null)
            schemaPattern = Pattern.compile(groups.get(6));
        Iterator<TableName> iter = names.iterator();
        while (iter.hasNext()) {
            TableName name = iter.next();
            if (((schemaPattern != null) && 
                 !schemaPattern.matcher(name.getSchemaName()).find()) ||
                ((tablePattern != null) && 
                 !tablePattern.matcher(name.getTableName()).find()))
                iter.remove();
        }
        Collections.sort(names);
    	for (TableName name : names) {
            int id;
            Columnar table = ais.getColumnar(name);
            if (table.isTable())
                id = ((Table)table).getTableId();
            else {
                if (nonTableNames == null)
                    nonTableNames = new HashMap<Integer,TableName>(); 
                id = - (nonTableNames.size() + 1);
                nonTableNames.put(id, name);
            }
            messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
            messenger.writeShort(3); // 3 columns for this query
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
        server.setAttribute("psql_nonTableNames", nonTableNames);
        return nrows;
    }

    private Columnar getTableById(PostgresServerSession server, String group) {
        AkibanInformationSchema ais = server.getAIS();
        int id = Integer.parseInt(group);
        if (id < 0) {
            Map<Integer,TableName> nonTableNames = (Map<Integer,TableName>)
                server.getAttribute("psql_nonTableNames");
            if (nonTableNames != null) {
                TableName name = nonTableNames.get(id);
                if (name != null) {
                    return ais.getColumnar(name);
                }
            }
            return null;
        }
        else {
            return ais.getUserTable(id);
        }
    }

    private int psqlDescribeTables2Query(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        Columnar table = getTableById(server, groups.get(1));
        if (table == null) return 0;
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(8); // 8 columns for this query
        writeColumn(messenger, encoder, usePVals, // relchecks
                    0, INT2_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, // relkind
                    table.isView() ? "v" : "r", CHAR1_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, // relhasindex
                    hasIndexes(table) ? "t" : "f", CHAR1_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, // relhasrules
                    false, BOOL_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, // relhastriggers
                    false, BOOL_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, // relhasoids
                    false, BOOL_PG_TYPE);
        writeColumn(messenger, encoder, usePVals,
                    "", CHAR0_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, // reltablespacae
                    0, OID_PG_TYPE);
        messenger.sendMessage();
        return 1;
    }

    private int psqlDescribeTables2XQuery(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        Columnar table = getTableById(server, groups.get(1));
        if (table == null) return 0;
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(5); // 5 columns for this query
        writeColumn(messenger, encoder, usePVals, // relhasindex
                    hasIndexes(table) ? "t" : "f", CHAR1_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, // relkind
                    table.isView() ? "v" : "r", CHAR1_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, // relchecks
                    0, INT2_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, // reltriggers
                    0, INT2_PG_TYPE);
        writeColumn(messenger, encoder, usePVals, // relhasrules
                    false, BOOL_PG_TYPE);
        messenger.sendMessage();
        return 1;
    }

    private int psqlDescribeTables3Query(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        int nrows = 0;
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        Columnar table = getTableById(server, groups.get(2));
        if (table == null) return 0;
        boolean hasCollation = (groups.get(1) != null);
        for (Column column : table.getColumns()) {
            messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
            messenger.writeShort(hasCollation ? 6 : 5); // 5-6 columns for this query
            writeColumn(messenger, encoder, usePVals, // attname
                        column.getName(), IDENT_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, // format_type
                        column.getTypeDescription(), TYPNAME_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, 
                        null, CHAR0_PG_TYPE);
            // This should use BOOL_PG_TYPE, except that does true/false, not t/f.
            writeColumn(messenger, encoder, usePVals, // attnotnull
                        column.getNullable() ? "f" : "t", CHAR1_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, // attnum
                        column.getPosition(), INT2_PG_TYPE);
            if (hasCollation)
                writeColumn(messenger, encoder, usePVals, // attcollation
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
        Columnar table = getTableById(server, groups.get(1));
        if (table == null) return 0;
        return 0;
    }

    private int psqlDescribeIndexesQuery(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        int nrows = 0;
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        Columnar columnar = getTableById(server, groups.get(3));
        if ((columnar == null) || !columnar.isTable()) return 0;
        UserTable table = (UserTable)columnar;
        Map<String,Index> indexes = new TreeMap<String,Index>();
        for (Index index : table.getIndexesIncludingInternal()) {
            if (isAkibanPKIndex(index)) continue;
            indexes.put(index.getIndexName().getName(), index);
        }
        for (Index index : table.getGroup().getIndexes()) {
            if (table == index.leafMostTable()) {
                indexes.put(index.getIndexName().getName(), index);
            }
        }
        int ncols;
        if (groups.get(1) == null) {
            ncols = 4;
        }
        if (groups.get(2) == null) {
            ncols = 7;
        }
        else {
            ncols = 11;
        }
        for (Index index : indexes.values()) {
            messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
            messenger.writeShort(ncols); // 4-7-11 columns for this query
            writeColumn(messenger, encoder, usePVals, // relname
                        index.getIndexName().getName(), IDENT_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, // indisprimary
                        (index.getIndexName().getName().equals(Index.PRIMARY_KEY_CONSTRAINT)) ? "t" : "f", CHAR1_PG_TYPE);
            writeColumn(messenger, encoder, usePVals, // indisunique
                        (index.isUnique()) ? "t" : "f", CHAR1_PG_TYPE);
            if (ncols > 4) {
                writeColumn(messenger, encoder, usePVals, // indisclustered
                            false, BOOL_PG_TYPE);
                writeColumn(messenger, encoder, usePVals, // indisvalid
                            "t", CHAR1_PG_TYPE);
            }
            writeColumn(messenger, encoder, usePVals, // pg_get_indexdef
                        formatIndexdef(index, table), INDEXDEF_PG_TYPE);
            if (ncols > 7) {
                writeColumn(messenger, encoder, usePVals, // constraintdef
                            null, CHAR0_PG_TYPE);
                writeColumn(messenger, encoder, usePVals, // contype
                            null, CHAR0_PG_TYPE);
                writeColumn(messenger, encoder, usePVals, // condeferragble
                            false, BOOL_PG_TYPE);
                writeColumn(messenger, encoder, usePVals, // condeferred
                            false, BOOL_PG_TYPE);
            }
            if (ncols > 4) {
                writeColumn(messenger, encoder, usePVals, // reltablespace
                            0, OID_PG_TYPE);
            }
            messenger.sendMessage();
            nrows++;
            if ((maxrows > 0) && (nrows >= maxrows)) {
                break;
            }
        }
        return nrows;
    }

    private boolean hasIndexes(Columnar table) {
        if (!table.isTable())
            return false;
        Collection<? extends Index> indexes = ((UserTable)table).getIndexes();
        if (indexes.isEmpty())
            return false;
        if (indexes.size() > 1)
            return true;
        if (isAkibanPKIndex(indexes.iterator().next()))
            return false;
        return true;
    }

    private boolean isAkibanPKIndex(Index index) {
        List<IndexColumn> indexColumns = index.getKeyColumns();
        return ((indexColumns.size() == 1) && 
                indexColumns.get(0).getColumn().isAkibanPKColumn());
    }

    private String formatIndexdef(Index index, UserTable table) {
        StringBuilder str = new StringBuilder();
        // Postgres CREATE INDEX has USING method, btree|hash|gist|gin|...
        // That is where the client starts including output.
        // Only issue is that for PRIMARY KEY, it prints a comma in
        // anticipation of some method word before the column.
        str.append(" USING ");
        if (index.getIndexMethod() != Index.IndexMethod.NORMAL)
            str.append(index.getIndexMethod());
        str.append("(");
        boolean first = true;
        for (IndexColumn icolumn : index.getKeyColumns()) {
            Column column = icolumn.getColumn();
            if (first) {
                first = false;
            }
            else {
                str.append(", ");
            }
            if (column.getTable() != table) {
                str.append(column.getTable().getName().getTableName())
                   .append(".");
            }
            str.append(column.getName());
        }
        str.append(")");
        if (index.isGroupIndex()) {
            str.append(" USING " + index.getJoinType() + " JOIN");
        }
        return str.toString();
    }

    private int psqlDescribeViewQuery(PostgresServerSession server, PostgresMessenger messenger, int maxrows, boolean usePVals) throws IOException {
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
        Columnar table = getTableById(server, groups.get(1));
        if ((table == null) || !table.isView()) return 0;
        View view = (View)table;
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(1); // 1 column for this query
        writeColumn(messenger, encoder, usePVals, // pg_get_viewdef
                    view.getDefinition(), VIEWDEF_PG_TYPE);
        messenger.sendMessage();
        return 1;
    }

}
