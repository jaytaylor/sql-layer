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
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CharsetAndCollation;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.error.ProtobufReadException;
import com.akiban.server.error.ProtobufWriteException;
import com.akiban.util.GrowableByteBuffer;
import org.junit.Test;

import static com.akiban.ais.AISComparator.compareAndAssert;

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
        ProtobufReader reader = new ProtobufReader(bb);
        reader.load();
    }

    @Test(expected=ProtobufWriteException.class)
    public void writeBufferTooSmall() {
        GrowableByteBuffer bb = new GrowableByteBuffer(10);
        final AkibanInformationSchema inAIS = CAOIBuilderFiller.createAndFillBuilder(SCHEMA).ais();
        ProtobufWriter writer = new ProtobufWriter(bb);
        writer.save(inAIS);
    }


    private AkibanInformationSchema writeAndRead(AkibanInformationSchema inAIS) {
        GrowableByteBuffer bb = createByteBuffer();

        ProtobufWriter writer = new ProtobufWriter(bb);
        writer.save(inAIS);

        bb.flip();
        ProtobufReader reader = new ProtobufReader(bb);
        AkibanInformationSchema outAIS = reader.load();

        return outAIS;
    }

    private GrowableByteBuffer createByteBuffer() {
        return new GrowableByteBuffer(4096);
    }
}
