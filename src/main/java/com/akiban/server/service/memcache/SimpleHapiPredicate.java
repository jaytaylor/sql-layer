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

package com.akiban.server.service.memcache;

import com.akiban.ais.model.TableName;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.hapi.HapiUtils;
import com.akiban.util.ArgumentValidation;

public class SimpleHapiPredicate implements HapiPredicate {
    private final TableName tableName;
    private final String columnName;
    private final Operator op;
    private final String value;

    public SimpleHapiPredicate(TableName tableName, String columnName, Operator op, String value) {
        ArgumentValidation.notNull("table name", tableName);
        ArgumentValidation.notNull("column name", columnName);
        ArgumentValidation.notNull("operator", op);
        this.tableName = tableName;
        this.columnName = columnName;
        this.op = op;
        this.value = value;
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public Operator getOp() {
        return op;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return HapiUtils.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        return o == this || o instanceof HapiPredicate && HapiUtils.equals(this, (HapiPredicate) o);
    }

    @Override
    public int hashCode() {
        return HapiUtils.hashCode(this);
    }
}
