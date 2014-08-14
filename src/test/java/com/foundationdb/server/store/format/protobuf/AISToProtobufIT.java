/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.store.format.protobuf;

import com.foundationdb.protobuf.ProtobufDecompiler;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.protobuf.CommonProtobuf.ProtobufRowFormat;
import com.foundationdb.sql.aisddl.TableDDL;
import com.foundationdb.sql.aisddl.AlterTableDDL;
import com.foundationdb.sql.parser.AlterTableNode;
import com.foundationdb.sql.parser.CreateTableNode;
import com.foundationdb.sql.parser.DropTableNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.RenameNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import org.junit.Test;
import org.junit.runner.RunWith;
import static com.foundationdb.sql.TestBase.*;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(NamedParameterizedRunner.class)
public class AISToProtobufIT extends ITBase
{
    private static final String SCHEMA = "test";
    private static final File RESOURCE_DIR = 
        new File("src/test/resources/" + 
                 AISToProtobufIT.class.getPackage().getName().replace('.', '/'));
    private static final String UUID_REGEX =
        "(\")[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
    private final List<File> files;

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
        Pattern pattern = Pattern.compile("(.*)-\\d+.sql");
        Map<String,List<File>> cases = new TreeMap<>();
        for (File file : RESOURCE_DIR.listFiles()) {
            Matcher matcher = pattern.matcher(file.getName());
            if (matcher.matches()) {
                String caseName = matcher.group(1);
                List<File> entry = cases.get(caseName);
                if (entry == null) {
                    entry = new ArrayList<>();
                    cases.put(caseName, entry);
                }
                entry.add(file);
            }
        }
        Comparator<File> numericOrder = new Comparator<File>() {
            @Override
            public int compare(File f1, File f2) {
                return Integer.compare(getNumber(f1), getNumber(f2));
            }

            private int getNumber(File f) {
                String name = f.getName();
                int start = name.lastIndexOf('-') + 1;
                int end = name.lastIndexOf('.');
                return Integer.parseInt(name.substring(start, end));
            }
        };
        Collection<Parameterization> result = new ArrayList<>(cases.size());
        for (Map.Entry<String,List<File>> entry : cases.entrySet()) {
            Collections.sort(entry.getValue(), numericOrder);
            result.add(Parameterization.create(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    public AISToProtobufIT(List<File> files) {
        this.files = files;
    }
    
    @Test
    public void testAIS() throws Exception {
        FileDescriptorSet set = null;
        for (File file : files) {
            String sql = fileContents(file);
            runDDL(sql);
            AISToProtobuf ais2p = new AISToProtobuf(ProtobufRowFormat.Type.GROUP_MESSAGE, set);
            for (Group group : ais().getGroups().values()) {
                if (group.getName().getSchemaName().equals(SCHEMA)) {
                    ais2p.addGroup(group);
                }
            }
            set = ais2p.build();
            StringBuilder proto = new StringBuilder();
            new ProtobufDecompiler(proto).decompile(set);
            String actual = proto.toString();
            String expected = null;
            File expectedFile = changeSuffix(file, ".proto");
            if (expectedFile.exists()) {
                expected = fileContents(expectedFile);
            }
            String fullCaseName = file.getName().replace("\\.sql$", "");
            if (expected == null) {
                fail(fullCaseName + " no expected result given. actual='" + actual + "'");
            }
            else {
                assertEqualsWithoutPattern(fullCaseName, expected, actual, UUID_REGEX);
            }
        }
    }

    protected void runDDL(String sql) throws Exception {
        for (StatementNode stmt : new SQLParser().parseStatements(sql)) {
            switch (stmt.getNodeType()) {
            case NodeTypes.CREATE_TABLE_NODE:
                TableDDL.createTable(ddl(), session(), SCHEMA, (CreateTableNode)stmt, null);
                break;
            case NodeTypes.DROP_TABLE_NODE:
                TableDDL.dropTable(ddl(), session(), SCHEMA, (DropTableNode)stmt, null);
                break;
            case NodeTypes.ALTER_TABLE_NODE:
                AlterTableDDL.alterTable(ddl(), dml(), session(), SCHEMA, (AlterTableNode)stmt, null);
                break;
            case NodeTypes.RENAME_NODE:
                TableDDL.renameTable(ddl(), session(), SCHEMA, (RenameNode)stmt);
                break;
            default:
                fail("Unsupported statement: " + stmt);
            }
        }
    }
}
