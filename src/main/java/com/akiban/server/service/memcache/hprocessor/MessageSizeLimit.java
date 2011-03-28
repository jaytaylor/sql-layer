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

package com.akiban.server.service.memcache.hprocessor;

import com.akiban.server.RowData;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.util.ArgumentValidation;

public final class MessageSizeLimit implements ScanLimit
{
    private final int maxBytes;
    private int totalBytes = 0;

    public MessageSizeLimit(int maxBytes)
    {
        ArgumentValidation.isGTE("maxBytes", maxBytes, 0);
        this.maxBytes = maxBytes;
    }

    @Override
    public boolean limitReached(RowData previousRow)
    {
        boolean limitReached = false;
        if (maxBytes > 0) {
            totalBytes += previousRow.getBytes().length;
            limitReached = totalBytes >= maxBytes;
        }
        return limitReached;
    }
}
