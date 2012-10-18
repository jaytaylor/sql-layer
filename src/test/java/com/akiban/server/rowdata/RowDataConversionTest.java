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

package com.akiban.server.rowdata;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.ConverterTestUtils;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.typestests.ConversionSuite;
import com.akiban.server.types.typestests.ConversionTestBase;
import com.akiban.server.types.typestests.LinkedConversion;
import com.akiban.server.types.typestests.TestCase;
import com.akiban.util.ByteSource;
import com.akiban.util.Strings;
import com.akiban.util.WrappingByteSource;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

import static com.akiban.util.Strings.parseHex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(NamedParameterizedRunner.class)
public final class RowDataConversionTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        LongExtractor year = Extractors.getLongExtractor(AkType.YEAR);
        LongExtractor timestamp = Extractors.getLongExtractor(AkType.TIMESTAMP);
        LongExtractor time = Extractors.getLongExtractor(AkType.TIME);
        LongExtractor interval_millis = Extractors.getLongExtractor(AkType.INTERVAL_MILLIS);
        LongExtractor interval_month = Extractors.getLongExtractor(AkType.INTERVAL_MONTH);
        ConverterTestUtils.setGlobalTimezone("UTC");

        ConversionSuite<?> suite = ConversionSuite.build(new ConversionPair())
                // Double values
                .add(TestCase.forDouble(-0.0d, b(0x8000000000000000L)))
                .add(TestCase.forDouble(0.0d, b(0x0000000000000000L)))
                .add(TestCase.forDouble(-1.0d, b(0xBFF0000000000000L)))
                .add(TestCase.forDouble(1.0d, b(0x3FF0000000000000L)))
                .add(TestCase.forDouble(839573957392.29575739275d, b(0x42686F503D620977L)))
                .add(TestCase.forDouble(            -0.986730586093d, b(0xBFEF934C05A76F64L)))
                .add(TestCase.forDouble(428732459843.84344482421875d, b(0x4258F49C8AD0F5FBL)))
                .add(TestCase.forDouble(2.7182818284d, b(0x4005BF0A8B12500BL)))
                .add(TestCase.forDouble(-9007199250000000d, b(0xC33FFFFFFFB7A880L)))
                .add(TestCase.forDouble(        7385632847582937583d, b(0x43D99FC27C6C68D0L)))

                // BigDecimal -- values that were in the c_discount decimal(4,2) field
                .add(TestCase.forDecimal(d("0.38"), 4, 2, parseHex("0x8026")))
                .add(TestCase.forDecimal(d("0.44"), 4, 2, parseHex("0x802C")))
                .add(TestCase.forDecimal(d("0.01"), 4, 2, parseHex("0x8001")))
                .add(TestCase.forDecimal(d("0.33"), 4, 2, parseHex("0x8021")))
                .add(TestCase.forDecimal(d("0.04"), 4, 2, parseHex("0x8004")))
                .add(TestCase.forDecimal(d("0.50"), 4, 2, parseHex("0x8032")))
                .add(TestCase.forDecimal(d("0.45"), 4, 2, parseHex("0x802D")))
                .add(TestCase.forDecimal(d("0.14"), 4, 2, parseHex("0x800E")))
                .add(TestCase.forDecimal(d("0.03"), 4, 2, parseHex("0x8003")))
                // -- values that were in the c_balance decimal(12,2) field
                .add(TestCase.forDecimal(d("4673.96"), 12, 2, parseHex("0x800000124160")))
                .add(TestCase.forDecimal(d("8028.00"), 12, 2, parseHex("0x8000001F5C00")))
                .add(TestCase.forDecimal(d("1652.00"), 12, 2, parseHex("0x800000067400")))
                .add(TestCase.forDecimal(d("17588.70"), 12, 2, parseHex("0x80000044B446")))
                .add(TestCase.forDecimal(d("8542.35"), 12, 2, parseHex("0x800000215E23")))
                .add(TestCase.forDecimal(d("12703.18"), 12, 2, parseHex("0x800000319F12")))
                .add(TestCase.forDecimal(d("6009.00"), 12, 2, parseHex("0x800000177900")))
                .add(TestCase.forDecimal(d("18850.68"), 12, 2, parseHex("0x80000049A244")))
                .add(TestCase.forDecimal(d("6436.92"), 12, 2, parseHex("0x80000019245C")))
                // -- The only value that was in the c_ytd_payment decimal(12,2) field
                .add(TestCase.forDecimal(d("10.00"), 12, 2, parseHex("0x800000000A00")))
                // -- These next two aren't part of the bug, but we have them here anyway.
                // One is an example from mysql docs, and the other caused us a problem before.
                .add(TestCase.forDecimal(d("1234567890.1234"), 14, 4, parseHex("0x810DFB38D204D2")))
                .add(TestCase.forDecimal(d("90.1956251262"), 12, 10, parseHex("0xDA0BA900A602")))
                // -- zeros
                .add(TestCase.forDecimal(d("0"), 1, 0, parseHex("0x80")))
                .add(TestCase.forDecimal(d("0"), 4, 0, parseHex("0x8000")))
                .add(TestCase.forDecimal(d("0"), 8, 0, parseHex("0x80000000")))
                .add(TestCase.forDecimal(d("0"), 30, 0, parseHex("0x8000000000000000000000000000")))
                .add(TestCase.forDecimal(d("0.0"), 8, 1, parseHex("0x8000000000")))
                .add(TestCase.forDecimal(d("0.00"), 8, 2, parseHex("0x80000000")))
                .add(TestCase.forDecimal(d("0.0000"), 10, 4, parseHex("0x8000000000")))
                .add(TestCase.forDecimal(d("0.00000000"), 10, 8, parseHex("0x8000000000")))

                // Year
                .add(TestCase.forYear(year.getLong("0000"), b(0, 1)))
                .add(TestCase.forYear(year.getLong("1902"), b(2, 1)))
                .add(TestCase.forYear(year.getLong("1986"), b(86, 1)))
                .add(TestCase.forYear(year.getLong("2011"), b(111, 1)))
                .add(TestCase.forYear(year.getLong("2155"), b(255, 1)))
                
                // Timestamp
                .add(TestCase.forTimestamp(timestamp.getLong("0000-00-00 00:00:00"), b(0, 4)))
                .add(TestCase.forTimestamp(timestamp.getLong("1970-01-01 00:00:01"), b(1, 4)))
                .add(TestCase.forTimestamp(timestamp.getLong("2009-02-13 23:31:30"), b(1234567890, 4)))
                .add(TestCase.forTimestamp(timestamp.getLong("2009-02-13 23:31:30"), b(1234567890, 4)))
                .add(TestCase.forTimestamp(timestamp.getLong("2038-01-19 03:14:07"), b(2147483647, 4)))
                .add(TestCase.forTimestamp(timestamp.getLong("1986-10-28 00:00:00"), b(530841600, 4)))
                .add(TestCase.forTimestamp(timestamp.getLong("2011-04-10 18:34:00"), b(1302460440, 4)))

                // Interval millis
                .add(TestCase.forInterval_Millis(interval_millis.getLong("12345"), b(12345L, 8) ))
                .add(TestCase.forInterval_Month(interval_month.getLong("12345"), b(12345L, 8)))
                
                // Time
                .add(TestCase.forTime(time.getLong("00:00:00"), b(0, 3)))
                .add(TestCase.forTime(time.getLong("00:00:01"), b(1, 3)))
                .add(TestCase.forTime(time.getLong("-00:00:01"), b(-1, 3)))
                .add(TestCase.forTime(time.getLong("838:59:59"), b(8385959, 3)))
                .add(TestCase.forTime(time.getLong("-838:59:59"), b(-8385959, 3)))
                .add(TestCase.forTime(time.getLong("14:20:32"), b(142032, 3)))
                .add(TestCase.forTime(time.getLong("-147:21:01"), b(-1472101, 3)))
                
                // Strings (character encoding)
                .add(TestCase.forString("abc", 32, "UTF-8", parseHex("0x03616263")))
                .add(TestCase.forString("abc", 32, "ISO-8859-1", parseHex("0x03616263")))
                .add(TestCase.forString("cliché", 32, "UTF-8", parseHex("0x07636C696368C3A9")))
                .add(TestCase.forString("cliché", 32, "ISO-8859-1", parseHex("0x06636C696368E9")))
                .add(TestCase.forString("☃", 32, "UTF-8", parseHex("0x03E29883")))

                .suite();
        return filter(params(suite), new Predicate() {
            @Override
            public boolean include(TestCase<?> testCase) {
                return testCase.type() != AkType.BOOL;
            }
        });
    }

    public RowDataConversionTest(ConversionSuite<?> suite, int indexWithinSuite) {
        super(suite, indexWithinSuite);
    }

    private static final class ConversionPair implements LinkedConversion<ByteSource> {
        @Override
        public ValueSource linkedSource() {
            return source;
        }

        @Override
        public ValueTarget linkedTarget() {
            return target;
        }

        @Override
        public void checkPut(ByteSource expected) {
            ByteSource sourceBytes = source.byteSource();
            if (!expected.equals(sourceBytes)){
                assertEquals(Strings.hex(expected), Strings.hex(sourceBytes));
                fail(expected + " != " + sourceBytes);
            }
        }

        @Override
        public void setUp(TestCase<?> testCase) {
            if (testCase.type() == AkType.INTERVAL_MILLIS || testCase.type() == AkType.INTERVAL_MONTH)
                throw new UnsupportedOperationException();
            createEnvironment(testCase);
            byte[] bytes = new byte[128];
            target.bind(fieldDef, bytes, 0);
            source.bind(fieldDef, bytes);
        }

        @Override
        public void syncConversions() {
            source.setWidth(target.lastEncodedLength());
        }

        @Override
        public Set<? extends AkType> unsupportedTypes() {
            return EnumSet.of(AkType.INTERVAL_MILLIS, AkType.INTERVAL_MONTH);
        }

        private void createEnvironment(TestCase<?> testCase) {
            AkibanInformationSchema ais = AISBBasedBuilder.create("mySchema")
                    .userTable("testTable")
                    .colLong("id")
                    .pk("id")
                    .ais(false);
            Column col = Column.create(
                    ais.getUserTable("mySchema", "testTable"),
                    "c1",
                    1,
                    colType(testCase.type())
            );
            col.setTypeParameter1(testCase.param1());
            col.setTypeParameter2(testCase.param2());
            col.setCharset(testCase.charset());
            new SchemaFactory().buildRowDefs(ais);
            RowDef rowDef = ais.getTable("mySchema", "testTable").rowDef();
            fieldDef = rowDef.getFieldDef(rowDef.getFieldIndex("c1"));
        }

        private Type colType(AkType akType) {
            final String typeName;
            switch (akType) {
                case LONG:
                    typeName = Types.BIGINT.name().toUpperCase();
                    break;
                default:
                    typeName = akType.name();
                    break;
                case NULL:
                case UNSUPPORTED:
                    throw new UnsupportedOperationException(akType.name());
            }

            try {
                Field typesField = Types.class.getField(typeName);
                return (Type) typesField.get(null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private final TestableRowDataValueSource source = new TestableRowDataValueSource();
        private final RowDataValueTarget target = new RowDataValueTarget();
        private FieldDef fieldDef;
    }

    private static class TestableRowDataValueSource extends AbstractRowDataValueSource {
        @Override
        protected long getRawOffsetAndWidth() {
            return width;
        }

        @Override
        protected byte[] bytes() {
            return bytes;
        }

        @Override
        protected FieldDef fieldDef() {
            return fieldDef;
        }

        @Override
        public boolean isNull() {
            return width == 0;
        }

        void bind(FieldDef fieldDef, byte[] bytes) {
            this.fieldDef = fieldDef;
            this.bytes = bytes;
        }

        void setWidth(int width) {
            this.width = width;
            this.width <<= 32;
        }

        ByteSource byteSource() {
            long actualWidth = width;
            actualWidth >>>= 32;
            return new WrappingByteSource().wrap(bytes, 0, (int)actualWidth);
        }

        private long width;
        private byte[] bytes;
        private FieldDef fieldDef;
    }
    
    private static ByteSource b(long value) {
        return b(value, 8);
    }

    private static ByteSource b(long value, int bytes) {
        assert bytes > 0 && bytes <= 8 : bytes;
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value);
        return new WrappingByteSource().wrap(buffer.array(), 0, bytes);
    }

    private static BigDecimal d(String asString) {
        return new BigDecimal(asString);
    }
}
