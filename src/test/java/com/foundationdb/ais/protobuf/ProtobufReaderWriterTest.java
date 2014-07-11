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

package com.foundationdb.ais.protobuf;

import com.foundationdb.ais.CAOIBuilderFiller;
import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.error.ProtobufReadException;
import com.foundationdb.server.error.ProtobufWriteException;
import com.foundationdb.server.store.format.DummyStorageFormatRegistry;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.store.format.TestStorageDescription;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;
import org.junit.Test;

import java.nio.ByteBuffer;

import static com.foundationdb.ais.AISComparator.compareAndAssert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ProtobufReaderWriterTest {
    private final String SCHEMA = "test";

    @Test
    public void empty() {
        final AkibanInformationSchema inAIS = new AkibanInformationSchema();
        inAIS.freeze();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, false);
    }

    @Test
    public void caoi() {
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, false);
    }
    
    @Test
    public void caoiWithGroupIndex() {
        NewAISBuilder builder = CAOIBuilderFiller.createAndFillBuilder(SCHEMA);
        builder.groupIndex("iprice_odate", Index.JoinType.RIGHT).
                on(CAOIBuilderFiller.ITEM_TABLE, "unit_price").
                and(CAOIBuilderFiller.ORDER_TABLE, "order_date");
        
        final AkibanInformationSchema inAIS = builder.ais();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, false);
    }

    @Test
    public void caoiWithView() {
        NewAISBuilder builder = CAOIBuilderFiller.createAndFillBuilder(SCHEMA);
        builder.view("recent_orders").
            definition("CREATE VIEW recent_order AS SELECT * FROM order WHERE order_date > CURRENT_DATE - INTERVAL '30' DAY").
            references(CAOIBuilderFiller.ORDER_TABLE).
            colBigInt("order_id", false).
            colBigInt("customer_id", false).
                colInt("order_date", false);
        final AkibanInformationSchema inAIS = builder.ais();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, false);
    }

    @Test
    public void nonDefaultCharsetAndCollations() {
        // AIS char/col not serialized (will be on Schema when that exists)
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais(false);
        inAIS.getTable(SCHEMA, CAOIBuilderFiller.ORDER_TABLE).
                setCharsetAndCollation("utf16", "sv_se_ci");
        Column column = inAIS.getTable(SCHEMA, CAOIBuilderFiller.CUSTOMER_TABLE).getColumn("customer_name");
        TInstance type = column.getType();
        type = type.typeClass().instance(
                        type.attribute(StringAttribute.MAX_LENGTH),
                        StringFactory.Charset.LATIN1.ordinal(),
                        AkCollatorFactory.getAkCollator("latin1_swedish_ci").getCollationId(),
                        type.nullability());
        column.setType(type);
        inAIS.freeze();
        
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, false);
    }

    /*
     * Stubbed out parent, similar to how table creation from the adapter works
     */
    @Test
    public void partialParentWithFullChild() {
        final AkibanInformationSchema inAIS = new AkibanInformationSchema();
        
        Table stubCustomer = Table.create(inAIS, SCHEMA, "c", 1);
        TInstance bigint = typesRegistry().getTypeClass("MCOMPAT", "BIGINT").instance(false);
        Column cId = Column.create(stubCustomer, "id", 2, bigint);

        Table realOrder = Table.create(inAIS, SCHEMA, "o", 2);
        Column oId = Column.create(realOrder, "oid", 0, bigint);
        Column oCid = Column.create(realOrder, "cid", 1, bigint);
        TInstance date = typesRegistry().getTypeClass("MCOMPAT", "DATE").instance(true);
        Column.create(realOrder, "odate", 2, date);
        Index orderPK = TableIndex.create(inAIS, realOrder, Index.PRIMARY, 0, true, true, new TableName(SCHEMA, "pkey"));
        IndexColumn.create(orderPK, oId, 0, true, null);
        Index akFk = TableIndex.create(inAIS, realOrder, "_fk1", 1, false, false);
        IndexColumn.create(akFk, oCid, 0, true, null);
        Join coJoin = Join.create(inAIS, "co", stubCustomer, realOrder);
        JoinColumn.create(coJoin, cId, oCid);

        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, false);
    }

    /*
     * Stubbed out table, similar to how index creation from the adapter works
     */
    @Test
    public void partialTableWithIndexes() {
        final AkibanInformationSchema inAIS = new AkibanInformationSchema();

        Table stubCustomer = Table.create(inAIS, SCHEMA, "c", 1);
        TInstance varchar32 = typesRegistry().getTypeClass("MCOMPAT", "VARCHAR").instance(32, true);
        Column cFirstName = Column.create(stubCustomer, "first_name", 3, varchar32);
        Column cLastName = Column.create(stubCustomer, "last_name", 4, varchar32);
        TInstance int_null = typesRegistry().getTypeClass("MCOMPAT", "INT").instance(true);
        Column cPayment = Column.create(stubCustomer, "payment", 6, int_null);
        Index iName = TableIndex.create(inAIS, stubCustomer, "name", 2, false, false);
        IndexColumn.create(iName, cLastName, 0, true, null);
        IndexColumn.create(iName, cFirstName, 1, true, null);
        Index iPayment = TableIndex.create(inAIS, stubCustomer, "payment", 3, false, false);
        IndexColumn.create(iPayment, cPayment, 0, true, null);
        
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, false);
    }

    @Test
    public void caoiWithFullComparison() {
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, true);
    }

    @Test(expected=ProtobufReadException.class)
    public void missingRootTable() {
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais(false);
        inAIS.getTables().remove(TableName.create(SCHEMA, CAOIBuilderFiller.CUSTOMER_TABLE));
        inAIS.getSchema(SCHEMA).getTables().remove(CAOIBuilderFiller.CUSTOMER_TABLE);
        writeAndRead(inAIS);
    }

    @Test(expected=ProtobufReadException.class)
    public void readBufferTooSmall() {
        ByteBuffer bb = ByteBuffer.allocate(4096);
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        ProtobufWriter writer = new ProtobufWriter();
        writer.save(inAIS);
        writer.serialize(bb);

        bb.flip();
        bb.limit(bb.limit() / 2);
        new ProtobufReader(typesRegistry(), storageFormatRegistry()).loadBuffer(bb);
    }

    @Test(expected=ProtobufWriteException.class)
    public void writeBufferTooSmall() {
        ByteBuffer bb = ByteBuffer.allocate(10);
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        ProtobufWriter writer = new ProtobufWriter();
        writer.save(inAIS);
        writer.serialize(bb);
    }

    // bug971833
    @Test
    public void tableWithNoDeclaredPK() {
        // CREATE TABLE t1(valid BOOLEAN, state CHAR(2))
        final String TABLE = "t1";
        AISBuilder builder = new AISBuilder();
        builder.table(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "valid", 0, typesRegistry().getTypeClass("MCOMPAT", "TINYINT").instance(true), false, null, null);
        builder.column(SCHEMA, TABLE, "state", 1, typesRegistry().getTypeClass("MCOMPAT", "CHAR").instance(2, true), false, null, null);
        builder.createGroup(TABLE, SCHEMA);
        builder.addTableToGroup(TABLE, SCHEMA, TABLE);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        // AIS does not have to be validate-able to be serialized (this is how it comes from adapter)
        final AkibanInformationSchema inAIS = builder.akibanInformationSchema();
        final Table t1_1 = inAIS.getTable(SCHEMA, TABLE);
        assertNull("Source table should not have declared PK", t1_1.getPrimaryKey());
        assertNotNull("Source table should have internal PK", t1_1.getPrimaryKeyIncludingInternal());

        // Serialized AIS did not create an internal column, PK
        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        Table t1_2 = outAIS.getTable(SCHEMA, TABLE);
        assertNull("Deserialized should not have declared PK", t1_2.getPrimaryKey());
        assertNotNull("Deserialized should have internal PK", t1_2.getPrimaryKeyIncludingInternal());

        compareAndAssert(inAIS, outAIS, false);

        // Now add an internal PK and run through again
        t1_1.endTable(builder.getNameGenerator());
        assertNull("Source table should not have declared PK", t1_1.getPrimaryKey());
        assertNotNull("Source table should have internal PK", t1_1.getPrimaryKeyIncludingInternal());

        outAIS = writeAndRead(inAIS);
        t1_2 = outAIS.getTable(SCHEMA, TABLE);
        assertNull("Deserialized should not have declared PK", t1_2.getPrimaryKey());
        assertNotNull("Deserialized should have internal PK", t1_2.getPrimaryKeyIncludingInternal());

        compareAndAssert(inAIS, outAIS, false);
    }

    @Test
    public void writeWithRestrictedSchema() {
        final String SCHEMA1 = "test1";
        final String TABLE1 = "t1";
        final String SCHEMA2 = "test2";
        final String TABLE2 = "t2";
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator());
        builder.table(SCHEMA1, TABLE1).colInt("id", false).colString("v", 250).pk("id");
        builder.table(SCHEMA2, TABLE2).colInt("tid", false).pk("tid");
        AkibanInformationSchema inAIS = builder.ais();
        AkibanInformationSchema outAIS1 = writeAndRead(inAIS, SCHEMA1);
        assertEquals("Serialized AIS has just schema 1", "[" + SCHEMA1 + "]", outAIS1.getSchemas().keySet().toString());
        AkibanInformationSchema outAIS2 = writeAndRead(inAIS, SCHEMA2);
        assertEquals("Serialized AIS has just schema 2", "[" + SCHEMA2 + "]", outAIS2.getSchemas().keySet().toString());
    }

    @Test
    public void writeWithRestrictedSchemaNoMatch() {
        AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        AkibanInformationSchema outAIS = writeAndRead(inAIS, SCHEMA + "foobar");
        assertEquals("Serialized AIS has no schemas", "[]", outAIS.getSchemas().keySet().toString());
    }

    @Test
    public void loadMultipleBuffers() {
        final int COUNT = 3;
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator());
        builder.table(SCHEMA+0, "t0").colInt("id", false).pk("id");
        builder.table(SCHEMA+1, "t1").colBigInt("bid", false).colString("v", 32).pk("bid");
        builder.table(SCHEMA+2, "t2").colDouble("d").colInt("l").key("d_idx", "d");
        AkibanInformationSchema inAIS = builder.ais();


        ByteBuffer bbs[] = new ByteBuffer[COUNT];
        for(int i = 0; i < COUNT; ++i) {
            bbs[i] = createByteBuffer();
            ProtobufWriter writer = new ProtobufWriter(new ProtobufWriter.SingleSchemaSelector(SCHEMA+i));
            writer.save(inAIS);
            writer.serialize(bbs[i]);
        }

        AkibanInformationSchema outAIS = new AkibanInformationSchema();
        ProtobufReader reader = new ProtobufReader(typesRegistry(), storageFormatRegistry(), outAIS);
        for(int i = 0; i < COUNT; ++i) {
            bbs[i].flip();
            reader.loadBuffer(bbs[i]);
        }
        reader.loadAIS();

        compareAndAssert(inAIS, outAIS, true);
    }

    @Test
    public void groupAndIndexTreeNames() {
        final String GROUP_TREENAME = "foo";
        final String PARENT_PK_TREENAME = "bar";
        final String GROUP_INDEX_TREENAME = "zap";
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, typesTranslator());
        builder.table("parent").colInt("pid", false).colString("v", 32).pk("pid").key("v", "v");
        builder.table("child").colInt("cid", false).colInt("pid").pk("pid").joinTo("parent").on("pid", "pid");
        builder.groupIndex("v_cid", Index.JoinType.LEFT).on("parent", "v").and("child", "cid");

        AkibanInformationSchema inAIS = builder.ais();
        Table inParent = inAIS.getTable(SCHEMA, "parent");
        inParent.getGroup().setStorageDescription(new TestStorageDescription(inParent.getGroup(), GROUP_TREENAME));
        inParent.getGroup().getIndex("v_cid").setStorageDescription(new TestStorageDescription(inParent.getGroup().getIndex("v_cid"), GROUP_INDEX_TREENAME));
        inParent.getIndex("PRIMARY").setStorageDescription(new TestStorageDescription(inParent.getIndex("PRIMARY"), PARENT_PK_TREENAME));

        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, true);

        Table outParent = outAIS.getTable(SCHEMA, "parent");
        assertEquals("group treename", GROUP_TREENAME, outParent.getGroup().getStorageUniqueKey());
        assertEquals("parent pk treename", PARENT_PK_TREENAME, inParent.getIndex("PRIMARY").getStorageUniqueKey());
        assertEquals("group index treename", GROUP_INDEX_TREENAME, inParent.getGroup().getIndex("v_cid").getStorageUniqueKey());
    }

    @Test
    public void tableVersionNumber() {
        final String TABLE = "t1";
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, typesTranslator());
        builder.table(TABLE).colInt("pid", false).pk("pid");

        AkibanInformationSchema inAIS = builder.ais();
        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        assertSame("Table without version", null, outAIS.getTable(SCHEMA, TABLE).getVersion());

        final Integer VERSION = 5;
        inAIS.getTable(SCHEMA, TABLE).setVersion(VERSION);
        outAIS = writeAndRead(inAIS);
        assertEquals("Table with version", VERSION, outAIS.getTable(SCHEMA, TABLE).getVersion());
    }

    @Test
    public void sameRootTableNameTwoSchemas() {
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator());
        builder.table(SCHEMA+"1", "t").colInt("id", false).pk("id");
        builder.table(SCHEMA+"2", "t").colInt("id", false).pk("id");
        AkibanInformationSchema inAIS = builder.ais();
        writeAndRead(inAIS);
    }
    
    @Test
    public void sequenceSimple () {
        TableName seqName = new TableName (SCHEMA, "Sequence-1");
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator());
        builder.defaultSchema(SCHEMA);
        builder.sequence(seqName.getTableName());
        AkibanInformationSchema inAIS = builder.ais();
        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        assertNotNull(outAIS.getSequence(new TableName(SCHEMA, "Sequence-1")));
        Sequence sequence = outAIS.getSequence(new TableName(SCHEMA, "Sequence-1"));
        assertEquals(1, sequence.getStartsWith());
        assertEquals(1, sequence.getIncrement());
        assertEquals(Long.MIN_VALUE, sequence.getMinValue());
        assertEquals(Long.MAX_VALUE, sequence.getMaxValue());
        assertTrue(!sequence.isCycle());
    }
    
    @Test
    public void sequenceComplex() {
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator());
        builder.defaultSchema(SCHEMA);
        builder.sequence("sequence-2", 42, -2, true);
        AkibanInformationSchema inAIS = builder.ais();
        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        assertNotNull(outAIS.getSequence(new TableName(SCHEMA, "sequence-2")));
        Sequence sequence = outAIS.getSequence(new TableName(SCHEMA, "sequence-2"));
        assertEquals(42, sequence.getStartsWith());
        assertEquals(-2, sequence.getIncrement());
        assertTrue(sequence.isCycle());
    }
    
    @Test
    public void sequenceTree() {
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator());
        TableName seqName = new TableName (SCHEMA, "sequence-3");
        builder.defaultSchema(SCHEMA);
        builder.sequence("sequence-3", 42, -2, true);
        AkibanInformationSchema inAIS = builder.ais();
        Sequence inSeq = inAIS.getSequence(seqName);
        inSeq.setStorageDescription(new TestStorageDescription(inSeq, "sequence-3.tree"));
        
        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        assertNotNull(outAIS.getSequence(seqName));
        Sequence sequence = outAIS.getSequence(seqName);
        assertEquals ("sequence-3.tree", sequence.getStorageUniqueKey());
    }
    
    @Test 
    public void columnSequence() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, typesTranslator());
        TableName sequenceName = new TableName (SCHEMA, "sequence-4");
        builder.sequence(sequenceName.getTableName());
        builder.table("customers").
            colBigInt("customer_id", false).
            colString("customer_name", 100, false).
            pk("customer_id");
        AkibanInformationSchema inAIS = builder.unvalidatedAIS();
        Column idColumn = inAIS.getTable(new TableName (SCHEMA, "customers")).getColumn(0);
        idColumn.setDefaultIdentity(true);
        idColumn.setIdentityGenerator(inAIS.getSequence(sequenceName));
        
        AkibanInformationSchema outAIS = writeAndRead(builder.ais());
        
        assertNotNull(outAIS.getSequence(sequenceName));
        Column outColumn = outAIS.getTable(new TableName(SCHEMA, "customers")).getColumn(0);
        assertNotNull (outColumn.getDefaultIdentity());
        assertTrue (outColumn.getDefaultIdentity());
        assertNotNull (outColumn.getIdentityGenerator());
        assertSame (outColumn.getIdentityGenerator(), outAIS.getSequence(sequenceName));
    }

    @Test
    public void indexColumnIndexedLength() {
        final String TABLE = "t";
        final Integer INDEXED_LENGTH = 16;
        AISBuilder builder = new AISBuilder();
        builder.table(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "v", 0, typesRegistry().getTypeClass("MCOMPAT", "VARCHAR").instance(32, false), false, null, null);
        builder.index(SCHEMA, TABLE, "v");
        builder.indexColumn(SCHEMA, TABLE, "v", "v", 0, true, INDEXED_LENGTH);
        builder.createGroup(TABLE, SCHEMA);
        builder.addTableToGroup(TABLE, SCHEMA, TABLE);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        AkibanInformationSchema outAIS = writeAndRead(builder.akibanInformationSchema());
        Table table = outAIS.getTable(SCHEMA, TABLE);
        assertNotNull("found table", table);
        assertNotNull("has v index", table.getIndex("v"));
        assertEquals("v indexed length", INDEXED_LENGTH, table.getIndex("v").getKeyColumns().get(0).getIndexedLength());
    }

    @Test
    public void maxStorageSizeAndPrefixSize() {
        final String TABLE = "t";
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, typesTranslator());
        builder.table(TABLE).colBigInt("id");
        AkibanInformationSchema inAIS = builder.unvalidatedAIS();

        // Note: If storage* methods go away, or are non-null by default, that is *good* and these can go away
        Column inCol = inAIS.getTable(SCHEMA, TABLE).getColumn(0);
        assertNull("storedMaxStorageSize null by default", inCol.getMaxStorageSizeWithoutComputing());
        assertNull("storedPrefixSize null by default", inCol.getPrefixSizeWithoutComputing());

        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        Column outCol = outAIS.getTable(SCHEMA, TABLE).getColumn(0);
        assertNull("storedMaxStorageSize null preserved", outCol.getMaxStorageSizeWithoutComputing());
        assertNull("storedPrefixSize null preserved", outCol.getPrefixSizeWithoutComputing());

        inCol.getMaxStorageSize();
        inCol.getPrefixSize();

        outAIS = writeAndRead(inAIS);
        outCol = outAIS.getTable(SCHEMA, TABLE).getColumn(0);
        assertEquals("storedMaxStorageSize", Long.valueOf(8L), outCol.getMaxStorageSizeWithoutComputing());
        assertEquals("storedPrefixSize", Integer.valueOf(0), outCol.getPrefixSizeWithoutComputing());
    }

    @Test
    public void columnDefaultValue() {
        final String TABLE = "t";
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, typesTranslator());
        builder.table(TABLE).colBigInt("id");

        AkibanInformationSchema inAIS = builder.unvalidatedAIS();
        Column inCol = inAIS.getTable(SCHEMA, TABLE).getColumn("id");

        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        Column outCol = outAIS.getTable(SCHEMA, TABLE).getColumn("id");
        assertEquals("default defaultValue null", inCol.getDefaultValue(), outCol.getDefaultValue());

        inCol.setDefaultValue("100");
        outAIS = writeAndRead(inAIS);
        outCol = outAIS.getTable(SCHEMA, TABLE).getColumn("id");
        assertEquals("defaultValue", inCol.getDefaultValue(), outCol.getDefaultValue());
    }

    @Test
    public void procedureJava() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, typesTranslator());
        builder.sqljJar("myjar")
            // A file URL would vary by testing system. But don't check exists.
            .url("http://example.com/procs.jar", false);
        builder.procedure("PROC1")
            .language("java", Routine.CallingConvention.JAVA)
            .paramLongIn("x1")
            .paramLongIn("x2")
            .paramDoubleOut("d")
            .externalName("myjar", "com.acme.Procs", "proc1")
            .sqlAllowed(Routine.SQLAllowed.READS_SQL_DATA)
            .dynamicResultSets(2);
        
        AkibanInformationSchema inAIS = builder.ais();
        AkibanInformationSchema outAIS = writeAndRead(inAIS);

        Routine proc = outAIS.getRoutine(SCHEMA, "PROC1");
        assertNotNull(proc);
        
        SQLJJar jar = proc.getSQLJJar();
        assertNotNull(jar);
        assertEquals("myjar", jar.getName().getTableName());
        assertEquals("http://example.com/procs.jar", jar.getURL().toString());

        assertEquals("java", proc.getLanguage());
        assertEquals(Routine.CallingConvention.JAVA, proc.getCallingConvention());
        assertEquals(3, proc.getParameters().size());
        assertEquals("x1", proc.getParameters().get(0).getName());
        assertEquals(Parameter.Direction.IN, proc.getParameters().get(0).getDirection());
        assertEquals("BIGINT", proc.getParameters().get(0).getTypeName());
        assertEquals("x2", proc.getParameters().get(1).getName());
        assertEquals("BIGINT", proc.getParameters().get(1).getTypeName());
        assertEquals(Parameter.Direction.IN, proc.getParameters().get(1).getDirection());
        assertEquals("d", proc.getParameters().get(2).getName());
        assertEquals("DOUBLE", proc.getParameters().get(2).getTypeName());
        assertEquals(Parameter.Direction.OUT, proc.getParameters().get(2).getDirection());
        assertEquals("com.acme.Procs", proc.getClassName());
        assertEquals("proc1", proc.getMethodName());
        assertEquals(Routine.SQLAllowed.READS_SQL_DATA, proc.getSQLAllowed());
        assertEquals(2, proc.getDynamicResultSets());
    }

    @Test
    public void procedureLoadablePlan() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, typesTranslator());
        builder.procedure("PROC2")
            .language("java", Routine.CallingConvention.LOADABLE_PLAN)
            .externalName("com.acme.Procs", "proc1");
        
        AkibanInformationSchema inAIS = builder.ais();
        AkibanInformationSchema outAIS = writeAndRead(inAIS);

        Routine proc = outAIS.getRoutine(SCHEMA, "PROC2");
        
        assertEquals("java", proc.getLanguage());
        assertEquals(Routine.CallingConvention.LOADABLE_PLAN, proc.getCallingConvention());
        assertEquals(0, proc.getParameters().size());
    }

    private AkibanInformationSchema writeAndRead(AkibanInformationSchema inAIS) {
        return writeAndRead(inAIS, null);
    }

    private AkibanInformationSchema writeAndRead(AkibanInformationSchema inAIS, String restrictSchema) {
        ByteBuffer bb = createByteBuffer();

        final ProtobufWriter writer;
        if(restrictSchema == null) {
            writer = new ProtobufWriter();
        } else {
            writer = new ProtobufWriter(new ProtobufWriter.SingleSchemaSelector(restrictSchema));
        }
        writer.save(inAIS);
        writer.serialize(bb);

        bb.flip();
        ProtobufReader reader = new ProtobufReader(typesRegistry(), storageFormatRegistry()).loadBuffer(bb);
        return reader.loadAIS().getAIS();
    }

    private ByteBuffer createByteBuffer() {
        return ByteBuffer.allocate(4096);
    }

    private static TypesRegistry typesRegistry() {
        return TestTypesRegistry.MCOMPAT;
    }

    private static TypesTranslator typesTranslator() {
        return MTypesTranslator.INSTANCE;
    }

    private static StorageFormatRegistry storageFormatRegistry() {
        return DummyStorageFormatRegistry.create();
    }
}
