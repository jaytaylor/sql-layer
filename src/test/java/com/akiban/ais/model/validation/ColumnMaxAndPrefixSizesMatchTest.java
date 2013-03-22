
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.ColumnSizeMismatchException;
import org.junit.Test;

import java.util.Collections;

public class ColumnMaxAndPrefixSizesMatchTest {
    private final static String SCHEMA = "test";
    private final static String TABLE = "t";

    private static AkibanInformationSchema createAIS(Long maxStorage, Integer prefix) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        UserTable table = UserTable.create(ais, SCHEMA, TABLE, 1);
        Column.create(table, "id", 0, ais.getType("BIGINT"), false, null, null, null, null, maxStorage, prefix);
        return ais;
    }

    private static void validate(AkibanInformationSchema ais) {
        ais.validate(Collections.<AISValidation>singleton(new ColumnMaxAndPrefixSizesMatch())).throwIfNecessary();
    }

    @Test
    public void nulls() {
        validate(createAIS(null, null));
    }

    @Test
    public void correct() {
        validate(createAIS(8L, 0));
    }

    @Test(expected=ColumnSizeMismatchException.class)
    public void wroteMaxIsError() {
        validate(createAIS(50L, 0));
    }

    @Test(expected=ColumnSizeMismatchException.class)
    public void wrongPrefixIsError() {
        validate(createAIS(8L, 50));
    }

    @Test(expected=ColumnSizeMismatchException.class)
    public void wrongStorageAndPrefixIsError() {
        validate(createAIS(50L, 50));
    }
}
