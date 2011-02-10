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

package com.akiban.cserver.api;

import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.NoSuchTableException;

import java.io.IOException;
import java.util.Set;

public interface HapiProcessedGetRequest extends HapiGetRequest {
    Set<String> getProjectedTables();
    RowDef getRowDef(int tableId) throws IOException;
}
