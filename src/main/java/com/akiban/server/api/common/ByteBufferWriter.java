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

package com.akiban.server.api.common;

import java.nio.ByteBuffer;

public abstract class ByteBufferWriter {

    abstract protected void writeToBuffer(ByteBuffer output) throws Exception;

    public final int write(ByteBuffer output) throws Exception {
        final int startPos = output.position();
        writeToBuffer(output);
        return output.position() - startPos;
    }
}
