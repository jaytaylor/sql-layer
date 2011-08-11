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
import org.junit.Ignore;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertSame;

@Ignore("we need to ignore this test until RowDataConversion* have more control over the RowData state")
@RunWith(NamedParameterizedRunner.class)
public final class RowDataConversionTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ConversionSuite<?> suite = ConversionSuite.build(new ConversionPair())
                .add(TestCase.forDouble(                       -0.0d, 0x8000000000000000L))
                .add(TestCase.forDouble(                        0.0d, 0x0000000000000000L))
                .add(TestCase.forDouble(                       -1.0d, 0xBFF0000000000000L))
                .add(TestCase.forDouble(                        1.0d, 0x3FF0000000000000L))
                .add(TestCase.forDouble(   839573957392.29575739275d, 0x42686F503D620977L))
                .add(TestCase.forDouble(            -0.986730586093d, 0xBFEF934C05A76F64L))
                .add(TestCase.forDouble(428732459843.84344482421875d, 0x4258F49C8AD0F5FBL))
                .add(TestCase.forDouble(               2.7182818284d, 0x4005BF0A8B12500BL))
                .add(TestCase.forDouble(          -9007199250000000d, 0xC33FFFFFFFB7A880L))
                .add(TestCase.forDouble(        7385632847582937583d, 0x43D99FC27C6C68D0L))
                .suite();
        return params(suite);
    }
    
    public RowDataConversionTest(ConversionSuite<?> suite, int indexWithinSuite) {
        super(suite, indexWithinSuite);
    }

    private static final class ConversionPair implements LinkedConversion<Long> {
        @Override
        public ConversionSource linkedSource() {
            return source;
        }

        @Override
        public ConversionTarget linkedTarget() {
            return target;
        }

        @Override
        public void checkPut(Long expected) {
            // TODO...
        }

        @Override
        public void setUp(AkType type) {
            createEnvironment(type);
            target.bind(fieldDef, rowData);
            target.offset(0);
            source.bind(fieldDef, rowData);
        }

        @Override
        public void syncConversions() {
            assertSame("rowData", target.rowData(), source.rowData());
            assertSame("fieldDef", target.fieldDef(), source.fieldDef());
        }

        private void createEnvironment(AkType type) {
            AkibanInformationSchema ais = AISBBasedBuilder.create("mySchema")
                    .userTable("testTable")
                    .colLong("id")
                    .colDouble("c1", true)
                    .ais();
            RowDefCache rdc = new SchemaFactory().rowDefCache(ais);
            rowData = new RowData(new byte[1024]); // should be plenty!
            RowDef rowDef = rdc.getRowDef("mySchema", "testTable");
            fieldDef = rowDef.getFieldDef(rowDef.getFieldIndex("c1"));
        }

        private final RowDataConversionSource source = new RowDataConversionSource();
        private final RowDataConversionTarget target = new RowDataConversionTarget();
        private RowData rowData;
        private FieldDef fieldDef;
    }
}
