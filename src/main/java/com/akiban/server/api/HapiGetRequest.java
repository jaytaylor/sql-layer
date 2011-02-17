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

package com.akiban.server.api;

import com.akiban.ais.model.TableName;

import java.util.List;

public interface HapiGetRequest {
    /**
     * The name of the schema containing the tables involved in this request. Matches getUsingTable().getSchemaName().
     * @return The name of the schema containing the tables involved in this request.
     */
    String getSchema();

    /**
     * Rootmost table to be retrieved by this request.
     * @return The name (without schema) of the rootmost table to be retrieved.
     */
    String getTable();

    /**
     * The table whose columns are restricted by this request.
     * @return The schema and table name of the table whose columns are restricted by this request.
     */
    TableName getUsingTable();

    List<HapiPredicate> getPredicates();
}
