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

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.server.error.IndexColumnIsPartialException;
import org.junit.Test;

import java.util.Collections;

public class IndexColumnIsNotPartialTest {
    private final static String SCHEMA = "test";
    private final static String TABLE = "t";

    private static AkibanInformationSchema createAIS(long fullLen, Integer indexedLength) {
        AISBuilder builder = new AISBuilder();
        builder.userTable(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "v", 0, "VARCHAR", fullLen, null, false, false, null, null);
        builder.index(SCHEMA, TABLE, "v", false, Index.KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, TABLE, "v", "v", 0, true, indexedLength);
        builder.createGroup(TABLE, SCHEMA);
        builder.addTableToGroup(TABLE, SCHEMA, TABLE);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        return builder.akibanInformationSchema();
    }

    private static void validate(AkibanInformationSchema ais) {
        ais.validate(Collections.<AISValidation>singleton(new IndexColumnIsNotPartial())).throwIfNecessary();
    }

    @Test
    public void nullIndexedLengthIsValid() {
        validate(createAIS(32, null));
    }

    @Test(expected=IndexColumnIsPartialException.class)
    public void fullLengthIsInvalid() {
        validate(createAIS(32, 32));
    }

    @Test(expected=IndexColumnIsPartialException.class)
    public void partialLengthIsInvalid() {
        validate(createAIS(32, 16));
    }
}
