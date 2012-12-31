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
