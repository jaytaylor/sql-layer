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

package com.akiban.server.rowdata;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.ConversionTarget;
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

import static com.akiban.util.Strings.parseHex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(NamedParameterizedRunner.class)
public final class RowDataConversionTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ConversionSuite<?> suite = ConversionSuite.build(new ConversionPair())
                // Double values
                .add(TestCase.forDouble(-0.0d, b(0x8000000000000000L)))
                .add(TestCase.forDouble(0.0d, b(0x0000000000000000L)))
                .add(TestCase.forDouble(                       -1.0d, b(0xBFF0000000000000L)))
                .add(TestCase.forDouble(                        1.0d, b(0x3FF0000000000000L)))
                .add(TestCase.forDouble(   839573957392.29575739275d, b(0x42686F503D620977L)))
                .add(TestCase.forDouble(            -0.986730586093d, b(0xBFEF934C05A76F64L)))
                .add(TestCase.forDouble(428732459843.84344482421875d, b(0x4258F49C8AD0F5FBL)))
                .add(TestCase.forDouble(               2.7182818284d, b(0x4005BF0A8B12500BL)))
                .add(TestCase.forDouble(          -9007199250000000d, b(0xC33FFFFFFFB7A880L)))
                .add(TestCase.forDouble(        7385632847582937583d, b(0x43D99FC27C6C68D0L)))

                // BigDecimal -- values that were in the c_discount decimal(4,2) field
                .add(TestCase.forDecimal(d("0.38"), 4, 2, parseHex("0x8026")))
                .add(TestCase.forDecimal(d("0.44"), 4, 2,  parseHex("0x802C")))
                .add(TestCase.forDecimal(d("0.01"), 4, 2,  parseHex("0x8001")))
                .add(TestCase.forDecimal(d("0.33"), 4, 2,  parseHex("0x8021")))
                .add(TestCase.forDecimal(d("0.04"), 4, 2,  parseHex("0x8004")))
                .add(TestCase.forDecimal(d("0.50"), 4, 2,  parseHex("0x8032")))
                .add(TestCase.forDecimal(d("0.45"), 4, 2,  parseHex("0x802D")))
                .add(TestCase.forDecimal(d("0.14"), 4, 2,  parseHex("0x800E")))
                .add(TestCase.forDecimal(d("0.03"), 4, 2,  parseHex("0x8003")))
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
                .add(TestCase.forDecimal(d("0.00000000"), 10, 8, parseHex("0x8000000000000000000000000000")))
                
                .suite();
        return params(suite);
    }

    public RowDataConversionTest(ConversionSuite<?> suite, int indexWithinSuite) {
        super(suite, indexWithinSuite);
    }

    private static final class ConversionPair implements LinkedConversion<ByteSource> {
        @Override
        public ConversionSource linkedSource() {
            return source;
        }

        @Override
        public ConversionTarget linkedTarget() {
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
            createEnvironment(testCase);
            byte[] bytes = new byte[128];
            target.bind(fieldDef, bytes, 0);
            source.bind(fieldDef, bytes);
        }

        @Override
        public void syncConversions() {
            source.setWidth(target.lastEncodedLength());
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
            RowDefCache rdc = new SchemaFactory().rowDefCache(ais);
            RowDef rowDef = rdc.getRowDef("mySchema", "testTable");
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

        private final TestableRowDataConversionSource source = new TestableRowDataConversionSource();
        private final RowDataConversionTarget target = new RowDataConversionTarget();
        private FieldDef fieldDef;
    }

    private static class TestableRowDataConversionSource extends AbstractRowDataConversionSource {
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
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value);
        return new WrappingByteSource().wrap(buffer.array());
    }

    private static BigDecimal d(String asString) {
        return new BigDecimal(asString);
    }
}
