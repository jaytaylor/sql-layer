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

package com.foundationdb.server.rowdata;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Type;
import com.foundationdb.ais.model.Types;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.AkType;
import com.foundationdb.server.types.typestests.ConversionSuite;
import com.foundationdb.server.types.typestests.ConversionTestBase;
import com.foundationdb.server.types.typestests.LinkedConversion;
import com.foundationdb.server.types.typestests.TestCase;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.aksql.aktypes.AkInterval;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.pvalue.PValueSource;
import com.foundationdb.server.types.pvalue.PValueTarget;
import com.foundationdb.util.ByteSource;
import com.foundationdb.util.Strings;
import com.foundationdb.util.WrappingByteSource;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

import static com.foundationdb.util.Strings.parseHex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(NamedParameterizedRunner.class)
public final class RowDataConversionTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        //LongExtractor year = Extractors.getLongExtractor(AkType.YEAR);
        //LongExtractor timestamp = Extractors.getLongExtractor(AkType.TIMESTAMP);
        //LongExtractor time = Extractors.getLongExtractor(AkType.TIME);
        //LongExtractor interval_millis = Extractors.getLongExtractor(AkType.INTERVAL_MILLIS);
        //LongExtractor interval_month = Extractors.getLongExtractor(AkType.INTERVAL_MONTH);
        //ConverterTestUtils.setGlobalTimezone("UTC");
        TExecutionContext context = new TExecutionContext(null, null, null);
        
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
                
                .add(TestCase.forYear(MDatetimes.parseYear("0000", context), b(0, 1)))
                .add(TestCase.forYear(MDatetimes.parseYear("1902", context), b(2, 1)))
                .add(TestCase.forYear(MDatetimes.parseYear("1986", context), b(86, 1)))
                .add(TestCase.forYear(MDatetimes.parseYear("2011", context), b(111, 1)))
                .add(TestCase.forYear(MDatetimes.parseYear("2155", context), b(255, 1)))
                
                // Timestamp
                .add(TestCase.forTimestamp(MDatetimes.parseTimestamp("0000-00-00 00:00:00", "UTC", context), b(0,4)))
                .add(TestCase.forTimestamp(MDatetimes.parseTimestamp("1970-01-01 00:00:01", "UTC", context), b(1, 4)))
                .add(TestCase.forTimestamp(MDatetimes.parseTimestamp("2009-02-13 23:31:30", "UTC", context), b(1234567890, 4)))
                .add(TestCase.forTimestamp(MDatetimes.parseTimestamp("2009-02-13 23:31:30", "UTC", context), b(1234567890, 4)))
                .add(TestCase.forTimestamp(MDatetimes.parseTimestamp("2038-01-19 03:14:07", "UTC", context), b(2147483647, 4)))
                .add(TestCase.forTimestamp(MDatetimes.parseTimestamp("1986-10-28 00:00:00", "UTC", context), b(530841600, 4)))
                .add(TestCase.forTimestamp(MDatetimes.parseTimestamp("2011-04-10 18:34:00", "UTC", context), b(1302460440, 4)))

                // Interval millis
                .add(TestCase.forInterval_Millis(12345L, b(12345L, 8) ))
                .add(TestCase.forInterval_Month(12345L, b(12345L, 8)))
                
                // Time
                .add(TestCase.forTime(MDatetimes.parseTime("00:00:00", context), b(0, 3)))
                .add(TestCase.forTime(MDatetimes.parseTime("00:00:01", context), b(1, 3)))
                .add(TestCase.forTime(MDatetimes.parseTime("-00:00:01", context), b(-1, 3)))
                .add(TestCase.forTime(MDatetimes.parseTime("838:59:59", context), b(8385959, 3)))
                .add(TestCase.forTime(MDatetimes.parseTime("-838:59:59", context), b(-8385959, 3)))
                .add(TestCase.forTime(MDatetimes.parseTime("14:20:32", context), b(142032, 3)))
                .add(TestCase.forTime(MDatetimes.parseTime("-147:21:01", context), b(-1472101, 3)))
                
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
                return testCase.type().equals(AkBool.INSTANCE.instance(true));
            }
        });
    }

    public RowDataConversionTest(ConversionSuite<?> suite, int indexWithinSuite) {
        super(suite, indexWithinSuite);
    }

    private static final class ConversionPair implements LinkedConversion<ByteSource> {
        @Override
        public PValueSource linkedSource() {
            return source;
        }

        @Override
        public PValueTarget linkedTarget() {
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
            if (testCase.type().equals(AkInterval.SECONDS.instance(true)) ||
                    testCase.type().equals(AkInterval.MONTHS.instance(true)))
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
            col.setNullable(testCase.type().nullability());
            new SchemaFactory().buildRowDefs(ais);
            RowDef rowDef = ais.getTable("mySchema", "testTable").rowDef();
            fieldDef = rowDef.getFieldDef(rowDef.getFieldIndex("c1"));
        }

        private Type colType(TInstance tInstance) {
            final String typeName;
            typeName = tInstance.typeClass().name().unqualifiedName();
            
            try {
                Field typesField = Types.class.getField(typeName);
                return (Type) typesField.get(null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private final TestableRowDataValueSource source = new TestableRowDataValueSource();
        private final RowDataPValueTarget target = new RowDataPValueTarget();
        private FieldDef fieldDef;
    }

    private static class TestableRowDataValueSource extends AbstractRowDataPValueSource {
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
