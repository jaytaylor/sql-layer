
package com.akiban.server.rowdata;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.util.AkibanAppender;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public final class ConversionHelperBigDecimalTest {

    @Test
    public void fits() {
        check(14, 10, "-0.1333333000");
    }

    @Test
    public void truncateIntComponentPositive() {
        checkWrite(5, 3, "12345.133", "99.999");
    }

    @Test
    public void truncateIntComponentNegative() {
        checkWrite(5, 3, "-12345.133", "-99.999");
    }

    @Test
    public void truncateFractComponentPositive() {
        checkWrite(5, 3, "12.3455", "12.346");
    }

    @Test
    public void truncateFractComponentNegative() {
        checkWrite(5, 3, "-12.3455", "-12.346");
    }

    @Test
    public void addPrecisionWriteRead() {
        checkWrite(5, 3, "2.5", "2.500");
    }

    @Test
    public void normalizeTruncateNoInt() {
        checkNormalizeToString("1", 4, 4, ".9999");
    }

    @Test
    public void normalizeTruncateNoIntNegative() {
        checkNormalizeToString("-1", 4, 4, "-.9999");
    }

    @Test
    public void normalizeTruncateOnlyInt() {
        checkNormalizeToString("1000000", 4, 0, "9999");
    }

    @Test
    public void normalizeTruncateMixed() {
        checkNormalizeToString("1000000", 4, 2, "99.99");
    }

    @Test
    public void normalizeTruncateFractional() {
        checkNormalizeToString("1.234567", 4, 2, "1.23");
    }

    @Test
    public void normalizeAddPrecision() {
        checkNormalizeToString("2.5", 5, 2, "2.50");
    }

    private void check(int precision, int scale, String value) {
        checkWrite(precision, scale, value, value);
    }

    private void checkWrite(int precision, int scale, String value, String readAs) {
        SchemaFactory schemaFactory = new SchemaFactory("my_schema");
        String sql = String.format("CREATE TABLE dec_test(dec_col decimal(%d,%d))", precision, scale);
        AkibanInformationSchema ais = schemaFactory.aisWithRowDefs(sql);
        RowDef rowDef = ais.getTable("my_schema", "dec_test").rowDef();

        FieldDef fieldDef = rowDef.getFieldDef(0);
        assertEquals("fieldDef name", fieldDef.getName(), "dec_col"); // make sure we have the right field def

        BigDecimal bigDecimal = new BigDecimal(value);

        int expectedSize = fieldDef.getMaxStorageSize();
        byte[] bytes = new byte[expectedSize];
        int actualSize = ConversionHelperBigDecimal.fromObject(fieldDef, bigDecimal, bytes, 0);
        assertEquals("serialized size for " + bigDecimal, expectedSize, actualSize);

        StringBuilder sb = new StringBuilder();
        ConversionHelperBigDecimal.decodeToString(fieldDef, bytes, 0, AkibanAppender.of(sb));

        assertEquals("after conversion", readAs, sb.toString());
    }

    private void checkNormalizeToString(String in, int precision, int scale, String expected) {
        BigDecimal bigDecimal = new BigDecimal(in);
        String actual = ConversionHelperBigDecimal.normalizeToString(bigDecimal, precision, scale);
        assertEquals(String.format("%s (%d,%d)", in, precision, scale), expected, actual);
    }
}
