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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(NamedParameterizedRunner.class)
public final class RowDataConversionTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ConversionSuite<?> suite = ConversionSuite.build(new ConversionPair())
                .add(TestCase.forDouble(                       -0.0d, b(0x8000000000000000L)))
                .add(TestCase.forDouble(                        0.0d, b(0x0000000000000000L)))
                .add(TestCase.forDouble(                       -1.0d, b(0xBFF0000000000000L)))
                .add(TestCase.forDouble(                        1.0d, b(0x3FF0000000000000L)))
                .add(TestCase.forDouble(   839573957392.29575739275d, b(0x42686F503D620977L)))
                .add(TestCase.forDouble(            -0.986730586093d, b(0xBFEF934C05A76F64L)))
                .add(TestCase.forDouble(428732459843.84344482421875d, b(0x4258F49C8AD0F5FBL)))
                .add(TestCase.forDouble(               2.7182818284d, b(0x4005BF0A8B12500BL)))
                .add(TestCase.forDouble(          -9007199250000000d, b(0xC33FFFFFFFB7A880L)))
                .add(TestCase.forDouble(        7385632847582937583d, b(0x43D99FC27C6C68D0L)))
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
        public void setUp(AkType type) {
            createEnvironment(type);
            byte[] bytes = new byte[128];
            target.bind(fieldDef, bytes, 0);
            source.bind(fieldDef, bytes);
        }

        @Override
        public void syncConversions() {
            source.setWidth(target.lastEncodedLength());
        }

        private void createEnvironment(AkType type) {
            AkibanInformationSchema ais = AISBBasedBuilder.create("mySchema")
                    .userTable("testTable")
                    .colLong("id")
                    .pk("id")
                    .ais(false);
            Column col = Column.create(
                    ais.getUserTable("mySchema", "testTable"),
                    "c1",
                    1,
                    colType(type)
            );
            if (col.getType().equals(Types.VARCHAR) || col.getType().equals(Types.TEXT)|| col.getType().equals(Types.VARBINARY)) {
                col.setTypeParameter1(32L);
                col.setCharset("UTF-8");
            }
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
}
