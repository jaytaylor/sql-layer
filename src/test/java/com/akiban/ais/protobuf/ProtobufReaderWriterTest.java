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

package com.akiban.ais.protobuf;

import com.akiban.ais.CAOIBuilderFiller;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CharsetAndCollation;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.error.ProtobufReadException;
import com.akiban.server.error.ProtobufWriteException;
import com.akiban.util.GrowableByteBuffer;
import org.junit.Test;

import static com.akiban.ais.AISComparator.compareAndAssert;
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
            colLong("order_date", false);
        final AkibanInformationSchema inAIS = builder.ais();
        final AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, false);
    }

    @Test
    public void nonDefaultCharsetAndCollations() {
        // AIS char/col not serialized (will be on Schema when that exists)
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais(false);
        inAIS.getUserTable(SCHEMA, CAOIBuilderFiller.ORDER_TABLE).
                setCharsetAndCollation(CharsetAndCollation.intern("utf16", "utf16_slovak_ci"));
        inAIS.getUserTable(SCHEMA, CAOIBuilderFiller.CUSTOMER_TABLE).getColumn("customer_name").
                setCharsetAndCollation(CharsetAndCollation.intern("ujis", "ujis_japanese_ci"));
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
        
        UserTable stubCustomer = UserTable.create(inAIS, SCHEMA, "c", 1);
        Column cId = Column.create(stubCustomer, "id", 2, Types.BIGINT, false, null, null, null, null);

        UserTable realOrder = UserTable.create(inAIS, SCHEMA, "o", 2);
        Column oId = Column.create(realOrder, "oid", 0, Types.BIGINT, false, null, null, null, null);
        Column oCid = Column.create(realOrder, "cid", 1, Types.BIGINT, false, null, null, null, null);
        Column.create(realOrder, "odate", 2, Types.DATE, true, null, null, null, null);
        Index orderPK = TableIndex.create(inAIS, realOrder, Index.PRIMARY_KEY_CONSTRAINT, 0, true, Index.PRIMARY_KEY_CONSTRAINT);
        IndexColumn.create(orderPK, oId, 0, true, null);
        Index akFk = TableIndex.create(inAIS, realOrder, Index.GROUPING_FK_PREFIX + "_fk1", 1, false, Index.FOREIGN_KEY_CONSTRAINT);
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

        UserTable stubCustomer = UserTable.create(inAIS, SCHEMA, "c", 1);
        Column cFirstName = Column.create(stubCustomer, "first_name", 3, Types.VARCHAR, true, 32L, null, null, null);
        Column cLastName = Column.create(stubCustomer, "last_name", 4, Types.VARCHAR, true, 32L, null, null, null);
        Column cPayment = Column.create(stubCustomer, "payment", 6, Types.INT, true, null, null, null, null);
        Index iName = TableIndex.create(inAIS, stubCustomer, "name", 2, false, Index.KEY_CONSTRAINT);
        IndexColumn.create(iName, cLastName, 0, true, null);
        IndexColumn.create(iName, cFirstName, 1, true, null);
        Index iPayment = TableIndex.create(inAIS, stubCustomer, "payment", 3, false, Index.KEY_CONSTRAINT);
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
        inAIS.getUserTables().remove(TableName.create(SCHEMA, CAOIBuilderFiller.CUSTOMER_TABLE));
        inAIS.getSchema(SCHEMA).getUserTables().remove(CAOIBuilderFiller.CUSTOMER_TABLE);
        writeAndRead(inAIS);
    }

    @Test(expected=ProtobufReadException.class)
    public void readBufferTooSmall() {
        GrowableByteBuffer bb = new GrowableByteBuffer(4096);
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        ProtobufWriter writer = new ProtobufWriter(bb);
        writer.save(inAIS);

        bb.flip();
        bb.limit(bb.limit() / 2);
        new ProtobufReader().loadBuffer(bb);
    }

    @Test(expected=ProtobufWriteException.class)
    public void writeBufferTooSmall() {
        GrowableByteBuffer bb = new GrowableByteBuffer(10);
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        ProtobufWriter writer = new ProtobufWriter(bb);
        writer.save(inAIS);
    }

    // bug971833
    @Test
    public void tableWithNoDeclaredPK() {
        // CREATE TABLE t1(valid BOOLEAN, state CHAR(2))
        final String TABLE = "t1";
        AISBuilder builder = new AISBuilder();
        builder.userTable(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "valid", 0, "TINYINT", null, null, true, false, null, null);
        builder.column(SCHEMA, TABLE, "state", 1, "CHAR", 2L, null, true, false, null, null);
        builder.createGroup(TABLE, SCHEMA, "akiban_"+TABLE);
        builder.addTableToGroup(TABLE, SCHEMA, TABLE);

        // AIS does not have to be validate-able to be serialized (this is how it comes from adapter)
        final AkibanInformationSchema inAIS = builder.akibanInformationSchema();
        final UserTable t1_1 = inAIS.getUserTable(SCHEMA, TABLE);
        assertNull("Source table should not have declared PK", t1_1.getPrimaryKey());
        assertNull("Source table should have internal PK", t1_1.getPrimaryKeyIncludingInternal());

        // Serialized AIS did not create an internal column, PK
        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        UserTable t1_2 = outAIS.getUserTable(SCHEMA, TABLE);
        assertNull("Deserialized should not have declared PK", t1_2.getPrimaryKey());
        assertNull("Deserialized should have internal PK", t1_2.getPrimaryKeyIncludingInternal());

        compareAndAssert(inAIS, outAIS, false);

        // Now add an internal PK and run through again
        t1_1.endTable();
        assertNull("Source table should not have declared PK", t1_1.getPrimaryKey());
        assertNotNull("Source table should have internal PK", t1_1.getPrimaryKeyIncludingInternal());

        outAIS = writeAndRead(inAIS);
        t1_2 = outAIS.getUserTable(SCHEMA, TABLE);
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
        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(SCHEMA1, TABLE1).colLong("id", false).colString("v", 250).pk("id");
        builder.userTable(SCHEMA2, TABLE2).colLong("tid", false).pk("tid");
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
        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(SCHEMA+0, "t0").colLong("id", false).pk("id");
        builder.userTable(SCHEMA+1, "t1").colBigInt("bid", false).colString("v", 32).pk("bid");
        builder.userTable(SCHEMA+2, "t2").colDouble("d").colLong("l").key("d_idx", "d");
        AkibanInformationSchema inAIS = builder.ais();


        GrowableByteBuffer bbs[] = new GrowableByteBuffer[COUNT];
        for(int i = 0; i < COUNT; ++i) {
            bbs[i] = createByteBuffer();
            new ProtobufWriter(bbs[i], new ProtobufWriter.SingleSchemaSelector(SCHEMA+i)).save(inAIS);
        }

        AkibanInformationSchema outAIS = new AkibanInformationSchema();
        ProtobufReader reader = new ProtobufReader(outAIS);
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
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA);
        builder.userTable("parent").colLong("pid", false).colString("v", 32).pk("pid").key("v", "v");
        builder.userTable("child").colLong("cid", false).colLong("pid").pk("pid").joinTo("parent").on("pid", "pid");
        builder.groupIndex("v_cid", Index.JoinType.LEFT).on("parent", "v").and("child", "cid");

        AkibanInformationSchema inAIS = builder.ais();
        UserTable inParent = inAIS.getUserTable(SCHEMA, "parent");
        inParent.getGroup().setTreeName(GROUP_TREENAME);
        inParent.getGroup().getIndex("v_cid").setTreeName(GROUP_INDEX_TREENAME);
        inParent.getIndex("PRIMARY").setTreeName(PARENT_PK_TREENAME);

        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        compareAndAssert(inAIS, outAIS, true);

        UserTable outParent = outAIS.getUserTable(SCHEMA, "parent");
        assertEquals("group treename", GROUP_TREENAME, outParent.getGroup().getTreeName());
        assertEquals("parent pk treename", PARENT_PK_TREENAME, inParent.getIndex("PRIMARY").getTreeName());
        assertEquals("group index treename", GROUP_INDEX_TREENAME, inParent.getGroup().getIndex("v_cid").getTreeName());
    }

    @Test
    public void tableVersionNumber() {
        final String TABLE = "t1";
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA);
        builder.userTable(TABLE).colLong("pid", false).pk("pid");

        AkibanInformationSchema inAIS = builder.ais();
        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        assertSame("Table without version", null, outAIS.getUserTable(SCHEMA, TABLE).getVersion());

        final Integer VERSION = 5;
        inAIS.getUserTable(SCHEMA, TABLE).setVersion(VERSION);
        outAIS = writeAndRead(inAIS);
        assertEquals("Table with version", VERSION, outAIS.getUserTable(SCHEMA, TABLE).getVersion());
    }

    @Test
    public void sameRootTableNameTwoSchemas() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(SCHEMA+"1", "t").colLong("id", false).pk("id");
        builder.userTable(SCHEMA+"2", "t").colLong("id", false).pk("id");
        AkibanInformationSchema inAIS = builder.ais();
        writeAndRead(inAIS);
    }
    
    @Test
    public void sequenceSimple () {
        TableName seqName = new TableName (SCHEMA, "Sequence-1");
        NewAISBuilder builder = AISBBasedBuilder.create();
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
        assertNotNull (sequence.getTreeName());
        assertNull (sequence.getAccumIndex());
    }
    
    @Test
    public void sequenceComplex() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.defaultSchema(SCHEMA);
        builder.sequence("sequence-2", 42, -2, true);
        AkibanInformationSchema inAIS = builder.ais();
        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        assertNotNull(outAIS.getSequence(new TableName(SCHEMA, "sequence-2")));
        Sequence sequence = outAIS.getSequence(new TableName(SCHEMA, "sequence-2"));
        assertEquals(42, sequence.getStartsWith());
        assertEquals(-2, sequence.getIncrement());
        assertTrue(sequence.isCycle());
        assertNotNull (sequence.getTreeName());
        assertNull (sequence.getAccumIndex());
    }
    
    @Test
    public void sequenceTree() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        TableName seqName = new TableName (SCHEMA, "sequence-3");
        builder.defaultSchema(SCHEMA);
        builder.sequence("sequence-3", 42, -2, true);
        AkibanInformationSchema inAIS = builder.ais();
        Sequence inSeq = inAIS.getSequence(seqName);
        inSeq.setTreeName("sequence-3.tree");
        inSeq.setAccumIndex(3);
        
        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        assertNotNull(outAIS.getSequence(seqName));
        Sequence sequence = outAIS.getSequence(seqName);
        assertEquals ("sequence-3.tree", sequence.getTreeName());
    }
    
    @Test 
    public void columnSequence() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA);
        TableName sequenceName = new TableName (SCHEMA, "sequence-4");
        builder.sequence(sequenceName.getTableName());
        builder.userTable("customers").
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
        builder.userTable(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "v", 0, "VARCHAR", 32L, null, false, false, null, null);
        builder.index(SCHEMA, TABLE, "v", false, Index.KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, TABLE, "v", "v", 0, true, INDEXED_LENGTH);
        builder.createGroup(TABLE, SCHEMA, "_akiban"+TABLE);
        builder.addTableToGroup(TABLE, SCHEMA, TABLE);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        AkibanInformationSchema outAIS = writeAndRead(builder.akibanInformationSchema());
        UserTable table = outAIS.getUserTable(SCHEMA, TABLE);
        assertNotNull("found table", table);
        assertNotNull("has v index", table.getIndex("v"));
        assertEquals("v indexed length", INDEXED_LENGTH, table.getIndex("v").getKeyColumns().get(0).getIndexedLength());
    }

    @Test
    public void maxStorageSizeAndPrefixSize() {
        final String TABLE = "t";
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA);
        builder.userTable(TABLE).colBigInt("id");
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
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA);
        builder.userTable(TABLE).colBigInt("id");

        AkibanInformationSchema inAIS = builder.unvalidatedAIS();
        Column inCol = inAIS.getUserTable(SCHEMA, TABLE).getColumn("id");

        AkibanInformationSchema outAIS = writeAndRead(inAIS);
        Column outCol = outAIS.getUserTable(SCHEMA, TABLE).getColumn("id");
        assertEquals("default defaultValue null", inCol.getDefaultValue(), outCol.getDefaultValue());

        inCol.setDefaultValue("100");
        outAIS = writeAndRead(inAIS);
        outCol = outAIS.getUserTable(SCHEMA, TABLE).getColumn("id");
        assertEquals("defaultValue", inCol.getDefaultValue(), outCol.getDefaultValue());
    }

    @Test
    public void procedureJava() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA);
        builder.sqljJar("myjar")
            // A file URL would vary by testing system.
            .url("http://software.akiban.com/procs.jar");
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
        assertEquals("http://software.akiban.com/procs.jar", jar.getURL().toString());

        assertEquals("java", proc.getLanguage());
        assertEquals(Routine.CallingConvention.JAVA, proc.getCallingConvention());
        assertEquals(3, proc.getParameters().size());
        assertEquals("x1", proc.getParameters().get(0).getName());
        assertEquals(Parameter.Direction.IN, proc.getParameters().get(0).getDirection());
        assertEquals(Types.BIGINT, proc.getParameters().get(0).getType());
        assertEquals("x2", proc.getParameters().get(1).getName());
        assertEquals(Types.BIGINT, proc.getParameters().get(1).getType());
        assertEquals(Parameter.Direction.IN, proc.getParameters().get(1).getDirection());
        assertEquals("d", proc.getParameters().get(2).getName());
        assertEquals(Types.DOUBLE, proc.getParameters().get(2).getType());
        assertEquals(Parameter.Direction.OUT, proc.getParameters().get(2).getDirection());
        assertEquals("com.acme.Procs", proc.getClassName());
        assertEquals("proc1", proc.getMethodName());
        assertEquals(Routine.SQLAllowed.READS_SQL_DATA, proc.getSQLAllowed());
        assertEquals(2, proc.getDynamicResultSets());
    }

    @Test
    public void procedureLoadablePlan() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA);
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
        GrowableByteBuffer bb = createByteBuffer();

        final ProtobufWriter writer;
        if(restrictSchema == null) {
            writer = new ProtobufWriter(bb);
        } else {
            writer = new ProtobufWriter(bb, new ProtobufWriter.SingleSchemaSelector(restrictSchema));
        }
        writer.save(inAIS);

        bb.flip();
        ProtobufReader reader = new ProtobufReader().loadBuffer(bb);
        AkibanInformationSchema outAIS = reader.loadAIS().getAIS();

        return outAIS;
    }

    private GrowableByteBuffer createByteBuffer() {
        return new GrowableByteBuffer(4096);
    }
}
