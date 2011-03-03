/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.ddl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.akiban.ais.model.AkibanInformationSchema;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.ddl.SchemaDef.CName;
import com.akiban.ais.ddl.SchemaDef.ColumnDef;
import com.akiban.ais.ddl.SchemaDef.IndexDef;
import com.akiban.ais.ddl.SchemaDef.UserTableDef;
import com.akiban.ais.io.MessageTarget;
import com.akiban.ais.io.Reader;
import com.akiban.ais.io.Writer;
import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Source;
import com.akiban.util.Strings;

/**
 * TODO - remove this class.  As of 1/5/2011 this class is no longer used by
 * server component. When studio and other components no longer need it, 
 * this class should be deleted.  Its logic has been divided into (a)
 * addition of parse capability in SchemaDef, and (b) new class
 * SchemaDefToAis.
 * 
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
        private ParseException(Exception cause) {
            super(cause.getMessage(), cause);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(DDLSource.class.getName());

    private final static String SCHEMA_FILE_NAME = "src/test/resources/xxxxxxxx_schema.ddl";
    private final static int MAX_AIS_SIZE = 1048576;
    private final static String BINARY_FORMAT = "binary";
    private final static String SQL_FORMAT = "sql";

    private SchemaDef schemaDef;
    private Map<String, ColumnDef> columnDefMap = new HashMap<String, ColumnDef>();
    private final Map<CName, String> groupNames = new HashMap<CName, String>();
    private final Map<JoinName, String> joinNames = new HashMap<JoinName, String>();

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
        final AkibanInformationSchema ais = source.buildAISFromFile(iFileName);
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
    }

    public AkibanInformationSchema buildAISFromFile(final String fileName) throws Exception {
        this.schemaDef = SchemaDef.parseSchemaFromFile(fileName);
        return schemaDefToAis();
    }

    public AkibanInformationSchema buildAISFromString(final String schema) throws Exception {
        this.schemaDef = SchemaDef.parseSchema(schema);
        return schemaDefToAis();
    }

    public UserTableDef parseCreateTable(final String createTableStatement) throws Exception {
        this.schemaDef = new SchemaDef();
        try {
            return schemaDef.parseCreateTable(createTableStatement);
        } catch(RuntimeException e) {
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
        return "_akiban_" + groupName.getName();
    }

    /**
     * Schema name generator for groups
     * 
     * @return "akiban_objects"
     */
    private String groupSchemaName() {
        return "akiban_objects";
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
     * Index name generator. This code actually has to (and does) emulate the
     * MySQL algorithm for assigning unspecified names.
     * 
     * @param namingMap
     * @param indexDef
     * @return
     */
    private static String generateIndexName(
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
                            def.defaultAutoIncrement(), groupSchemaName(),
                            groupTableName, groupColumnName, null, null,
                            // TODO: This isn't really correct: if collation is specified but not 
                            // charset, the collation's default charset should be used.
                            // But to do this we need to add a charset/collation database.
                            def.charset == null ? AkibanInformationSchema.DEFAULT_CHARSET : def.charset,
                            def.collate == null ? AkibanInformationSchema.DEFAULT_COLLATION : def.collate));
                    columnReceiver.receive(map(column, groupSchemaName(),
                            groupTableName, groupColumnName, def.gposition,
                            def.typeName, longValue(def.typeParam1),
                            longValue(def.typeParam2), def.nullable,
                            def.defaultAutoIncrement(), null, null, null,
                            null, null,
                            // TODO: This isn't really correct: if collation is specified but not 
                            // charset, the collation's default charset should be used.
                            // But to do this we need to add a charset/collation database.
                            def.charset == null ? AkibanInformationSchema.DEFAULT_CHARSET : def.charset,
                            def.collate == null ? AkibanInformationSchema.DEFAULT_COLLATION : def.collate));
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
                final UserTableDef childTable = schemaDef.getUserTableMap().get(childTableName);
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
                int columnIndex = 0;
                if (!utDef.getPrimaryKey().isEmpty()) {
                    indexId++;
                    columnIndex = 0;
                    for (final String pk : utDef.primaryKey) {
                        final ColumnDef columnDef = columnDefMap.get(utDef.name
                                + "." + pk);
                        final String gtn = groupTableName(groupName);
                        indexColumnReceiver
                                .receive(map(indexColumn, tableName.getSchema(),
                                        tableName.getName(), Index.PRIMARY_KEY_CONSTRAINT,
                                        columnDef.name, columnIndex, true, null));
                        indexColumnReceiver.receive(map(indexColumn,
                                groupSchemaName(), gtn,
                                userPKIndexName(gtn, utDef, indexId),
                                mangledColumnName(utDef.name, columnDef.name),
                                columnIndex, true, null));
                        columnIndex++;
                    }
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
                final UserTableDef utDef = schemaDef.getUserTableMap().get(tableName);
                final Map<String, AtomicInteger> indexNamingMap = new HashMap<String, AtomicInteger>();
                if (!utDef.getPrimaryKey().isEmpty()) {
                    indexId++;
                    final String gtn = groupTableName(groupName);
                    indexReceiver.receive(map(index,
                                              tableName.getSchema(),
                                              tableName.getName(),
                                              Index.PRIMARY_KEY_CONSTRAINT,
                                              indexId,
                                              Index.PRIMARY_KEY_CONSTRAINT,
                                              true));
                    indexReceiver.receive(map(index,
                                              groupSchemaName(),
                                              gtn,
                                              userPKIndexName(gtn, utDef, indexId),
                                              indexId,
                                              "INDEX",
                                              false));
                }
                for (final IndexDef indexDef : utDef.indexes) {
                    String indexType = "INDEX";
                    boolean unique = false;
                    for (final SchemaDef.IndexQualifier qualifier : indexDef.qualifiers) {
                        if (qualifier.equals(SchemaDef.IndexQualifier.FOREIGN_KEY)) {
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
        int tableId = 0;

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
        // Types are now added implicitly by the AkibanInformationSchema
        // constructor.
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

    public AkibanInformationSchema buildAISFromBuilder(final String string) throws RecognitionException, Exception
    {
        this.schemaDef = SchemaDef.parseSchema(string);
        return schemaDefToAis();
    }

    private AkibanInformationSchema schemaDefToAis() throws Exception {
        SchemaDefToAis toAis = new SchemaDefToAis(this.schemaDef, false);
        return toAis.getAis();
    }
}
