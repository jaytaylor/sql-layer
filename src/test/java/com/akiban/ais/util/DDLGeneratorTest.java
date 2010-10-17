package com.akiban.ais.util;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibaInformationSchema;
import org.junit.Test;

import static junit.framework.Assert.*;

public final class DDLGeneratorTest {

    @Test
    public void testCreateTable() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "table");
        builder.column("schema", "table", "col", 0, "decimal unsigned", 11L, 3L, true, false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("myGroup", "akiba_objects", "_group0");
        builder.addTableToGroup("myGroup", "schema", "table");
        builder.groupingIsComplete();

        AkibaInformationSchema ais = builder.akibaInformationSchema();
        DDLGenerator generator = new DDLGenerator();

        assertEquals("group table",
                "create table `akiba_objects`.`_group0`(`table$col` decimal(11, 3) unsigned  ) engine = akibadb",
                generator.createTable(ais.getGroup("myGroup").getGroupTable()));
    }
}
