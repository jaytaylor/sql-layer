package com.akiban.ais.ddl;

import com.akiban.ais.ddl.SchemaDef.CName;
import com.akiban.ais.ddl.SchemaDef.ColumnDef;
import com.akiban.ais.ddl.SchemaDef.IndexDef;
import com.akiban.ais.ddl.SchemaDef.UserTableDef;
import com.akiban.ais.io.MessageTarget;
import com.akiban.ais.io.Reader;
import com.akiban.ais.io.Writer;
import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Source;
import org.antlr.runtime.*;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class reads the CREATE TABLE statements in a mysqldump file, plus
 * annotations to denote the group structure. There is neither an attempt to
 * fully parse the DDL, nor to handle syntactic variations. The purpose of this
 * class is to facilitate creating AIS instances from existing MySQL databases
 * prior to the arrival of the Control Center's implementation.
 * 
 * There is no error handling, and this is not a general-purpose parser. The
 * format of the text file must be exact, especially with respect to spaces,
 * back ticks, etc.
 * 
 * See the xxxxxxxx_schema.sql file in src/test/resources for an example of the
 * syntax.
 * 
 * @author peter
 * 
 */
public class DDLSource extends Source {

    public class ParseException extends Exception {
        private ParseException(SchemaDef.SchemaDefException cause) {
            super(cause.getMessage(), cause);
        }
    }

    private static final Log LOG = LogFactory.getLog(DDLSource.class.getName());

    private final static String SCHEMA_FILE_NAME = "src/test/resources/xxxxxxxx_schema.ddl";
    private final static int MAX_AIS_SIZE = 1048576;
    private final static String BINARY_FORMAT = "binary";
    private final static String SQL_FORMAT = "sql";

    private String ddlSourceName = "<not specified>";
    private SchemaDef schemaDef;
    private Map<String, ColumnDef> columnDefMap = new HashMap<String, ColumnDef>();
    private final Map<CName, String> groupNames = new HashMap<CName, String>();
    private final Map<JoinName, String> joinNames = new HashMap<JoinName, String>();
    private final int baseId;

    public static class StringStream extends ANTLRStringStream {

        public StringStream(final String string) {
            super(string);
        }

        @Override
        public int LA(int i) {
            if (i == 0) {
                return 0; // undefined
            }
            if (i < 0) {
                i++; // e.g., translate LA(-1) to use offset 0
                if ((p + i - 1) < 0) {
                    return CharStream.EOF; // invalid; no char
                    // before first
                    // char
                }
            }

            if ((p + i - 1) >= n) {

                return CharStream.EOF;
            }
            return Character.toLowerCase(data[p + i - 1]);
        }
    }

    public static class FileStream extends ANTLRFileStream {

        public FileStream(final String fileName) throws IOException {
            super(fileName);
        }

        @Override
        public int LA(int i) {
            if (i == 0) {
                return 0; // undefined
            }
            if (i < 0) {
                i++; // e.g., translate LA(-1) to use offset 0
                if ((p + i - 1) < 0) {
                    return CharStream.EOF; // invalid; no char
                    // before first
                    // char
                }
            }

            if ((p + i - 1) >= n) {

                return CharStream.EOF;
            }
            return Character.toLowerCase(data[p + i - 1]);
        }
    }

    public static void main(final String[] args) throws Exception {

        String iFileName = SCHEMA_FILE_NAME;
        String oFileName = "/tmp/"
                + new File(iFileName).getName().split("\\.")[0] + ".out";
        String format = SQL_FORMAT;

        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption("i", "input-file", true, "default input file = "
                + iFileName);
        options.addOption("o", "output-file", true, "default output file = "
                + oFileName);
        options.addOption("f", "format", true,
                "valid values are sql and binary; the default is sql");
        options.addOption("h", "help", false, "print this message");

        try {
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("DDLSource", options);
                return;
            }

            if (line.hasOption("input-file")) {
                iFileName = line.getOptionValue("input-file");
            }

            if (line.hasOption("output-file")) {
                oFileName = line.getOptionValue("output-file");
            }

            if (line.hasOption("format")) {
                format = line.getOptionValue("format");
                format = format.toLowerCase();
                if (format.compareTo(BINARY_FORMAT) != 0
                        && format.compareTo(SQL_FORMAT) != 0) {
                    System.out.println("invald format option " + format
                            + "; using default = " + SQL_FORMAT);
                    format = SQL_FORMAT;
                }
            }
        } catch (org.apache.commons.cli.ParseException exp) {
            System.out.println("Unexpected exception:" + exp.getMessage());
        }

        final DDLSource source = new DDLSource();
        final AkibaInformationSchema ais = source.buildAIS(iFileName);
        // AISPrinter.print(ais);

        if (format.compareTo(SQL_FORMAT) == 0) {
            final PrintWriter pw = new PrintWriter(new FileWriter(oFileName));
            SqlTextTarget target = new SqlTextTarget(pw);
            new Writer(target).save(ais);
            target.writeGroupTableDDL(ais);
            pw.close();
        } else {
            assert format.compareTo(BINARY_FORMAT) == 0;

            ByteBuffer rawAis = ByteBuffer.allocate(MAX_AIS_SIZE);
            rawAis.order(ByteOrder.LITTLE_ENDIAN);
            new Writer(new MessageTarget(rawAis)).save(ais);
            rawAis.flip();

            boolean append = false;
            File file = new File(oFileName);
            try {
                FileChannel wChannel = new FileOutputStream(file, append)
                        .getChannel();
                wChannel.write(rawAis);
                wChannel.close();
            } catch (IOException e) {
                throw new Exception("rarrrgh");
            }
        }
    }

    private static class JoinName {
        private final CName parentName;
        private final CName childName;

        private JoinName(CName parentName, CName childName) {
            this.parentName = parentName;
            this.childName = childName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JoinName joinName = (JoinName) o;
            if (!childName.equals(joinName.childName)) {
                return false;
            }
            if (!parentName.equals(joinName.parentName)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = parentName.hashCode();
            result = 31 * result + childName.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Join[parent: " + parentName + " -> child: " + childName
                    + ']';
        }
    }

    public DDLSource() {
        this(0);
    }

    public DDLSource(int baseId) {
        this.baseId = baseId;
    }

    public AkibaInformationSchema buildAIS(final String fileName)
            throws Exception {
        ddlSourceName = fileName;
        return buildAIS(new FileStream(fileName));
    }

    public AkibaInformationSchema buildAISFromString(final String string)
            throws Exception {
        return buildAIS(new StringStream(string));
    }

    public static SchemaDef parseSchemaDef(final String string)
            throws Exception {
        DDLSource instance = new DDLSource();
        instance.parseSchemaDef(new StringStream(string));
        return instance.schemaDef;
    }

    private void parseSchemaDef(final ANTLRStringStream stringStream)
            throws RecognitionException {
        DDLSourceLexer lex = new DDLSourceLexer(stringStream);
        CommonTokenStream tokens = new CommonTokenStream(lex);
        final DDLSourceParser tsparser = new DDLSourceParser(tokens);
        this.schemaDef = new SchemaDef();
        tsparser.schema(schemaDef);
        if (tsparser.getNumberOfSyntaxErrors() > 0) {
            throw new RuntimeException("DDLSource reported a syntax error in: "
                    + ddlSourceName);
        }
        addImpliedGroups(schemaDef.getMasterSchemaName());
        computeColumnMapAndPositions();
    }

    private AkibaInformationSchema buildAIS(final ANTLRStringStream stringStream)
            throws Exception {
        parseSchemaDef(stringStream);
        AkibaInformationSchema ais = new Reader(this)
                .load(new AkibaInformationSchema());
        return ais;
    }

    public UserTableDef parseCreateTable(final String createTableStatement)
            throws Exception {
        DDLSourceLexer lex = new DDLSourceLexer(new StringStream(
                createTableStatement));
        CommonTokenStream tokens = new CommonTokenStream(lex);
        final DDLSourceParser tsparser = new DDLSourceParser(tokens);
        try {
            final SchemaDef schemaDef = new SchemaDef();
            tsparser.table_spec(schemaDef);
            if (tsparser.getNumberOfSyntaxErrors() > 0) {
                throw new RuntimeException("DDLSource reported a syntax error in: "
                        + ddlSourceName);
            }
            return schemaDef.getCurrentTable();
        } catch (SchemaDef.SchemaDefException e) {
            throw new ParseException(e);
        }
    }

    @Override
    public void close() throws Exception {
    }

    private Long longValue(final String s) {
        return s == null ? null : Long.valueOf(Long.parseLong(s));
    }

    /**
     * Name generator for Group tables.
     * 
     * @param groupName
     * @return
     */
    private String groupTableName(final CName groupName) {
        return "_akiba_" + groupName.getName();
    }

    private CName groupName(final CName rootTable) {
        String ret = groupNames.get(rootTable);
        if (ret == null) {
            ret = rootTable.getName();
            for (int i = 0; groupNames.containsValue(ret); ++i) {
                ret = rootTable.getName() + '$' + i;
            }
            groupNames.put(rootTable, ret);
        }
        return new CName("akiba_objects", ret);
    }

    /**
     * Schema name generator for groups
     * 
     * @return "akiba_objects"
     */
    private String groupSchemaName() {
        return "akiba_objects";
    }

    /**
     * Group column name generator for group columns. TODO: Needs to handle long
     * schemaName / tableName / columnName combos.
     * 
     * @param tableName
     * @param columnName
     * @return
     */
    private String mangledColumnName(final CName tableName,
            final String columnName) {
        return tableName.getName() + "$" + columnName;
    }

    /**
     * Index name generator.
     * 
     * @param utdef
     * @param indexDef
     * @return
     */
    private String mangledIndexName(final UserTableDef utdef,
            final IndexDef indexDef) {
        return utdef.name.getName() + "$" + indexDef.name;
    }

    /**
     * Tests an FOREIGN KEY index definition to determine whether it represents
     * a group-defining relationship.
     * 
     * @param indexDef
     * @return
     */
    private static boolean isAkiban(final IndexDef indexDef) {
        return SchemaDef.isAkiban(indexDef);
    }

    /**
     * Index name generator. This code actually has to (and does) emulate the
     * MySQL algorithm for assigning unspecified names.
     * 
     * @param namingMap
     * @param indexDef
     * @return
     */
    private String generateIndexName(
            final Map<String, AtomicInteger> namingMap, final IndexDef indexDef) {
        final String firstColumnName = indexDef.columns.get(0).columnName;
        AtomicInteger count = namingMap.get(firstColumnName);
        if (count == null) {
            count = new AtomicInteger(1);
            namingMap.put(firstColumnName, count);
            return firstColumnName;
        } else {
            return firstColumnName + "_" + (count.incrementAndGet());
        }
    }

    /**
     * Return List of names of user tables in a specified group sorted by
     * depth-first traversal of the parent-child relationships between those
     * tables.
     * 
     * @param groupName
     * @return sorted List of table names
     */
    List<CName> depthFirstSortedUserTables(final CName groupName) {
        final CName root = new CName(schemaDef, "", "");
        final Map<CName, SortedSet<CName>> hierarchy = new HashMap<CName, SortedSet<CName>>();
        final List<CName> tableList = new ArrayList<CName>();
        for (final CName tableName : schemaDef.getGroupMap().get(groupName)) {
            final UserTableDef utdef = schemaDef.getUserTableMap().get(
                    tableName);
            CName parent = utdef.parent == null ? root : utdef.parent.name;
            SortedSet<CName> children = hierarchy.get(parent);
            if (children == null) {
                children = new TreeSet<CName>();
                hierarchy.put(parent, children);
            }
            children.add(utdef.name);
        }
        traverseDepthFirstSortedTableMap(root, hierarchy, tableList);
        if (tableList.isEmpty()) {
            throw new IllegalStateException("Broken user table hiearchy: "
                    + hierarchy);
        }
        return tableList;
    }

    void traverseDepthFirstSortedTableMap(final CName parent,
            final Map<CName, SortedSet<CName>> hierarchy,
            final List<CName> tableList) {
        SortedSet<CName> siblings = hierarchy.get(parent);
        if (siblings != null) {
            for (final CName tableName : siblings) {
                tableList.add(tableName);
                traverseDepthFirstSortedTableMap(tableName, hierarchy,
                        tableList);
            }
        }
    }

    void computeColumnMapAndPositions() {
        for (final CName groupName : schemaDef.getGroupMap().keySet()) {
            int gposition = 0;
            final List<CName> tableList = depthFirstSortedUserTables(groupName);
            for (final CName tableName : tableList) {
                int uposition = 0;
                final UserTableDef utdef = schemaDef.getUserTableMap().get(
                        tableName);
                List<ColumnDef> columns = utdef.columns;
                for (final ColumnDef def : columns) {
                    def.gposition = gposition++;
                    def.uposition = uposition++;
                    columnDefMap.put(utdef.name + "." + def.name, def);
                }
            }
        }
    }

    @Override
    protected final void read(String typename, Receiver receiver)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readColumns(Receiver columnReceiver) throws Exception {
        for (final CName groupName : schemaDef.getGroupMap().keySet()) {
            final List<CName> tableList = depthFirstSortedUserTables(groupName);
            for (final CName tableName : tableList) {
                final UserTableDef utdef = schemaDef.getUserTableMap().get(
                        tableName);
                List<ColumnDef> columns = utdef.columns;
                for (final ColumnDef def : columns) {
                    String groupTableName = groupTableName(groupName);
                    String groupColumnName = mangledColumnName(tableName,
                            def.name);
                    columnReceiver.receive(map(column, tableName.getSchema(),
                            tableName.getName(), def.name, def.uposition,
                            def.typeName, longValue(def.typeParam1),
                            longValue(def.typeParam2), def.nullable,
                            longValue(def.autoincrement), groupSchemaName(),
                            groupTableName, groupColumnName, null, null,
                            // This isn't really correct
                            AkibaInformationSchema.DEFAULT_CHARSET,
                            AkibaInformationSchema.DEFAULT_COLLATION));
                    columnReceiver.receive(map(column, groupSchemaName(),
                            groupTableName, groupColumnName, def.gposition,
                            def.typeName, longValue(def.typeParam1),
                            longValue(def.typeParam2), def.nullable,
                            longValue(def.autoincrement), null, null, null,
                            null, null,
                            // This isn't really correct
                            AkibaInformationSchema.DEFAULT_CHARSET,
                            AkibaInformationSchema.DEFAULT_COLLATION));
                }
            }
        }
    }

    @Override
    public void readJoins(Receiver joinReceiver) throws Exception {
        for (final CName groupName : schemaDef.getGroupMap().keySet()) {
            for (final CName tableName : schemaDef.getGroupMap().get(groupName)) {
                final UserTableDef utdef = schemaDef.getUserTableMap().get(
                        tableName);
                if (utdef.parent != null) {
                    final CName parentName = utdef.parent.getCName();
                    joinReceiver.receive(map(join,
                            joinName(utdef.parent.name, tableName),
                            parentName.getSchema(), parentName.getName(),
                            tableName.getSchema(), tableName.getName(),
                            groupName.getName(), 0, 0, 0));
                }
            }
        }
    }

    @Override
    public void readJoinColumns(Receiver joinColumnReceiver) throws Exception {
        for (final CName groupName : schemaDef.getGroupMap().keySet()) {
            final List<CName> tableList = depthFirstSortedUserTables(groupName);
            for (final CName childTableName : tableList) {
                final UserTableDef childTable = schemaDef.getUserTableMap()
                        .get(childTableName);
                if (childTable.parent != null) {
                    CName parentName = childTable.parent.name;
                    final UserTableDef parentTable = schemaDef
                            .getUserTableMap().get(parentName);
                    assert childTable.parentJoinColumns.size() == parentTable.primaryKey
                            .size();
                    Iterator<String> childJoinColumnNameScan = childTable.childJoinColumns
                            .iterator();
                    Iterator<String> parentJoinColumnNameScan = childTable.parentJoinColumns
                            .iterator();
                    while (childJoinColumnNameScan.hasNext()
                            && parentJoinColumnNameScan.hasNext()) {
                        String childJoinColumnName = childJoinColumnNameScan
                                .next();
                        String parentJoinColumnName = parentJoinColumnNameScan
                                .next();
                        joinColumnReceiver.receive(map(joinColumn,
                                joinName(parentName, childTableName),
                                parentName.getSchema(), parentName.getName(),
                                parentJoinColumnName,
                                childTableName.getSchema(),
                                childTableName.getName(), childJoinColumnName));
                    }
                }
            }
        }
    }

    @Override
    public void readGroups(Receiver groupReceiver) throws Exception {
        for (final CName groupName : schemaDef.getGroupMap().keySet()) {
            groupReceiver.receive(map(group, groupName.getName()));
        }
    }

    @Override
    public void readIndexColumns(Receiver indexColumnReceiver) throws Exception {
        for (final CName groupName : schemaDef.getGroupMap().keySet()) {
            int indexId = 0;
            for (final CName tableName : schemaDef.getGroupMap().get(groupName)) {
                final UserTableDef utDef = schemaDef.getUserTableMap().get(
                        tableName);
                
                final Map<String, AtomicInteger> indexNamingMap = new HashMap<String, AtomicInteger>();
                indexId++;
                int columnIndex = 0;
                for (final String pk : utDef.primaryKey) {
                    final ColumnDef columnDef = columnDefMap.get(utDef.name
                            + "." + pk);
                    final String gtn = groupTableName(groupName);
                    indexColumnReceiver
                            .receive(map(indexColumn, tableName.getSchema(),
                                    tableName.getName(), "PRIMARY",
                                    columnDef.name, columnIndex, true, null));
                    indexColumnReceiver.receive(map(indexColumn,
                            groupSchemaName(), gtn,
                            userPKIndexName(gtn, utDef, indexId),
                            mangledColumnName(utDef.name, columnDef.name),
                            columnIndex, true, null));
                    columnIndex++;
                }
                for (final IndexDef indexDef : utDef.indexes) {
                    columnIndex = 0;
                    indexId++;
                    for (final SchemaDef.IndexColumnDef indexColumnDef : indexDef.columns) {
                        String indexColumnName = indexColumnDef.columnName;
                        final ColumnDef columnDef;
                        if (indexColumnName.contains(".")) {
                            columnDef = columnDefMap.get(indexColumnName);
                        } else {
                            columnDef = columnDefMap.get(utDef.name + "."
                                    + indexColumnName);
                        }
                        if (columnDef == null) {
                            LOG.error("Can't find index column named "
                                    + indexColumnName + " in user table "
                                    + utDef.name);
                            continue;
                        }
                        if (indexDef.name == null) {
                            indexDef.name = generateIndexName(indexNamingMap,
                                    indexDef);
                        }
                        indexColumnReceiver.receive(map(indexColumn,
                                tableName.getSchema(), tableName.getName(),
                                indexDef.name, columnDef.name, columnIndex,
                                !indexColumnDef.descending,
                                indexColumnDef.indexedLength));
                        indexColumnReceiver.receive(map(indexColumn,
                                groupSchemaName(), groupTableName(groupName),
                                mangledIndexName(utDef, indexDef),
                                mangledColumnName(utDef.name, columnDef.name),
                                columnIndex, !indexColumnDef.descending,
                                indexColumnDef.indexedLength));
                        columnIndex++;
                    }
                }
            }
        }
    }

    @Override
    public void readIndexes(Receiver indexReceiver) throws Exception {
        for (final CName groupName : schemaDef.getGroupMap().keySet()) {
            int indexId = 0;
            for (final CName tableName : schemaDef.getGroupMap().get(groupName)) {
                final UserTableDef utDef = schemaDef.getUserTableMap().get(
                        tableName);
                final Map<String, AtomicInteger> indexNamingMap = new HashMap<String, AtomicInteger>();
                indexId++;
                final String gtn = groupTableName(groupName);
                indexReceiver.receive(map(index, tableName.getSchema(),
                        tableName.getName(), "PRIMARY", indexId, "PRIMARY",
                        true));
                indexReceiver.receive(map(index, groupSchemaName(), gtn,
                        userPKIndexName(gtn, utDef, indexId), indexId, "INDEX", false));
                for (final IndexDef indexDef : utDef.indexes) {
                    String indexType = "INDEX";
                    boolean unique = false;
                    for (final SchemaDef.IndexQualifier qualifier : indexDef.qualifiers) {
                        if (qualifier
                                .equals(SchemaDef.IndexQualifier.FOREIGN_KEY)) {
                            indexType = "FOREIGN KEY";
                        }
                        if (qualifier.equals(SchemaDef.IndexQualifier.UNIQUE)) {
                            indexType = "UNIQUE";
                            unique = true;
                        }
                    }
                    if (indexDef.name == null) {
                        indexDef.name = generateIndexName(indexNamingMap,
                                indexDef);
                    }
                    indexId++;
                    indexReceiver.receive(map(index, tableName.getSchema(),
                            tableName.getName(), indexDef.name, indexId,
                            indexType, unique));
                    indexReceiver.receive(map(index, groupSchemaName(),
                            groupTableName(groupName),
                            mangledIndexName(utDef, indexDef), indexId,
                            "INDEX", false));
                }
            }
        }
    }

    @Override
    public void readTables(Receiver tableReceiver) throws Exception {
        int tableId = baseId;

        for (final CName groupName : schemaDef.getGroupMap().keySet()) {
            int groupTableId = ++tableId;
            for (final CName tableName : schemaDef.getGroupMap().get(groupName)) {
                tableId++;
                final UserTableDef utdef = schemaDef.getUserTableMap().get(
                        tableName);
                tableReceiver.receive(map(table, utdef.name.getSchema(),
                        utdef.name.getName(), "USER", tableId,
                        groupName.getName(), 0));
            }

            tableReceiver.receive(map(table, groupSchemaName(),
                    groupTableName(groupName), "GROUP", groupTableId,
                    groupName.getName(), 0));
        }
    }

    @Override
    public void readTypes(Receiver typeReceiver) throws Exception {
        // Types are now added implicitly by the AkibaInformationSchema
        // constructor.
    }

    public String getDdlSourceName() {
        return ddlSourceName;
    }

    public void setDdlSourceName(String ddlSourceName) {
        this.ddlSourceName = ddlSourceName;
    }

    private Map<String, Object> map(String typename, Object... values)
            throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        ModelObject modelObject = MetaModel.only().definition(typename);
        int i = 0;
        for (ModelObject.Attribute attribute : modelObject.attributes()) {
            map.put(attribute.name(), values[i++]);
        }
        return map;
    }

    // Assumes no more than one FK connecting tables, and that all tables are in
    // the same schema.
    private String joinName(CName parentName, CName childName) {
        JoinName joinName = new JoinName(parentName, childName);
        String ret = joinNames.get(joinName);
        if (ret == null) {
            ret = "join" + joinNames.size();
            joinNames.put(joinName, ret);
        }
        return ret;
    }

    private String userPKIndexName(String gtn, UserTableDef utDef, int indexId) {
        return String.format("%s$PK_%s", gtn, indexId);
    }

    /**
     * Hack to convert annotated FK constraints into group relationships. For
     * now this "supplements" the group syntax in DDLSource.
     */
    private void addImpliedGroups(final String schemaName) {
        final Set<CName> tablesInGroups = new HashSet<CName>();
        for (final Map.Entry<CName, SortedSet<CName>> entry : schemaDef
                .getGroupMap().entrySet()) {
            tablesInGroups.addAll(entry.getValue());
        }

        for (final CName userTableName : schemaDef.getUserTableMap().keySet()) {
            if (userTableName.getSchema().equals("akiba_objects")) {
                continue;
            }
            final UserTableDef utDef = addImpliedGroupTable(tablesInGroups,
                    userTableName);
            if (utDef == null) {
                System.out.println("No Group for table " + userTableName);
            }
        }
    }

    public static IndexDef getAkibanJoin(UserTableDef table) {
        IndexDef annotatedFK = null;
        for (final IndexDef indexDef : table.indexes) {
            if (isAkiban(indexDef)) {
                // TODO: Fragile - could be two or nore of these
                assert annotatedFK == null : "previous annotated FK: "
                        + annotatedFK;
                annotatedFK = indexDef;
            }
        }
        return annotatedFK;
    }

    //
    // TODO - the handling of schemaName throughout this class is just
    // atrocious. Big broom required.
    //
    private UserTableDef addImpliedGroupTable(final Set<CName> tablesInGroups,
            final CName userTableName) {
        final UserTableDef utDef = schemaDef.getUserTableMap().get(
                userTableName);
        if (utDef != null && "akibandb".equalsIgnoreCase(utDef.engine)
                && !tablesInGroups.contains(userTableName)) {
            IndexDef annotatedFK = getAkibanJoin(utDef);
            if (annotatedFK == null) {
                // No FK: this is a new root table so create a new Group
                final CName groupName = groupName(userTableName);
                final SortedSet<CName> members = new TreeSet<CName>();
                schemaDef.getGroupMap().put(groupName, members);
                utDef.groupName = groupName;
                members.add(userTableName);
            } else {
                utDef.parent = addImpliedGroupTable(tablesInGroups,
                        annotatedFK.referenceTable);
                if (utDef.parent != null) {
                    utDef.groupName = utDef.parent.groupName;
                    for (SchemaDef.IndexColumnDef childColumn : annotatedFK.columns) {
                        utDef.childJoinColumns.add(childColumn.columnName);
                    }
                    utDef.parentJoinColumns
                            .addAll(annotatedFK.referenceColumns);
                    final SortedSet<CName> members = schemaDef.getGroupMap()
                            .get(utDef.groupName);
                    members.add(userTableName);
                }
            }
            tablesInGroups.add(userTableName);
        }
        return utDef;
    }

    private final static String CREATE_TABLE = "CREATE TABLE ";
    private final static String IF_NOT_EXISTS = "IF NOT EXISTS ";

    public static String canonicalStatement(final String s) {
        final StringBuilder sb = new StringBuilder();
        boolean sc = false;
        boolean ws = false;
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (c > ' ') {
                if (ws) {
                    if (sb.length() > 0) {
                        sb.append(' ');
                    }
                    ws = false;
                }
                sb.append(c);
                sc = c == ';';
            } else {
                ws = true;
            }
        }
        if (!sc) {
            sb.append(';');
        }
        strip(sb, CREATE_TABLE);
        strip(sb, IF_NOT_EXISTS);
        return sb.toString();
    }

    private static void strip(StringBuilder sb, final String s) {
        if (sb.substring(0, s.length()).equalsIgnoreCase(s)) {
            sb.delete(0, s.length());
        }
    }
}
