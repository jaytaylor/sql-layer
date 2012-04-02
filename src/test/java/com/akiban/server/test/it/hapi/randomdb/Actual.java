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

package com.akiban.server.test.it.hapi.randomdb;

import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.memcache.ParsedHapiGetRequest;
import com.akiban.server.service.memcache.hprocessor.Scanrows;

import java.io.ByteArrayOutputStream;

class Actual
{
    public Actual(RCTortureIT test, ConfigurationService config, DXLService dxl)
    {
        this.test = test;
        this.config = config;
        this.dxl = dxl;
    }

    public String queryResult(int rootTable,
                              int predicateTable,
                              Column predicateColumn,
                              HapiPredicate.Operator comparison,
                              int literal) throws HapiRequestException
    {
        test.query = hapiQuery(rootTable, predicateTable, predicateColumn, comparison, literal);
        test.print(test.query);
        test.request = ParsedHapiGetRequest.parse(test.query);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(10000);
        Scanrows.instance(config, dxl).processRequest(test.testSession(), test.request, test.outputter, outputStream);
        return new String(outputStream.toByteArray());
    }

    private String hapiQuery(int rootTable,
                             int predicateTable,
                             Column predicateColumn,
                             HapiPredicate.Operator comparison,
                             int literal)
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(RCTortureIT.SCHEMA);
        buffer.append(RCTortureIT.COLON);
        if (rootTable == predicateTable) {
            buffer.append(test.table(predicateTable));
            buffer.append(RCTortureIT.COLON);
        } else {
            buffer.append(test.table(rootTable));
            buffer.append(RCTortureIT.COLON);
            buffer.append(RCTortureIT.OPEN);
            buffer.append(test.table(predicateTable));
            buffer.append(RCTortureIT.CLOSE);
        }
        buffer.append(predicateColumn.columnName());
        buffer.append(comparison);
        buffer.append(literal);
        return buffer.toString();
    }

    private final RCTortureIT test;
    protected final ConfigurationService config;
    private final DXLService dxl;
}
