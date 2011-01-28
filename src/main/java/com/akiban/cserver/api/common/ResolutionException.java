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

package com.akiban.cserver.api.common;


import com.akiban.ais.model.TableName;

import java.util.StringTokenizer;

public final class ResolutionException extends RuntimeException {
    public ResolutionException(Integer tableId, TableName tableName) {
        super(message(tableId, tableName, null));
    }

    public ResolutionException(Integer tableId, TableName tableName, String extraMessage) {
        super(message(tableId, tableName, extraMessage));
    }

    private static String message(Integer tableId, TableName tableName, String extraMessage) {
        StringBuilder sb = new StringBuilder();
        sb.append("id: ").append(tableId).append(", name: ").append(tableName);
        if (extraMessage != null) {
            sb.append(", message: ").append(extraMessage);
        }
        return sb.toString();
    }
}
