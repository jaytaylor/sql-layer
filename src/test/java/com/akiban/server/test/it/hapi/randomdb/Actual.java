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
