/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.api.common;

public final class WrongByteAllocationException extends RuntimeException {

    /**
     * Checks if the number of allocated bytes is the same as the number of
     * required bytes, and throws a WrongByteAllocationException if they're not.
     * @param allocated the number of bytes the invoker expected to allocate
     * @param required the number of bytes a method required
     * @throws WrongByteAllocationException if the two inputs aren't equal
     */
    public static void ifNotEqual(int allocated, int required) {
        if (allocated != required) {
            throw new WrongByteAllocationException(allocated, required);
        }
    }

    /**
     * Complains that the number of bytes allocated (by a method's invoker) is
     * not the same as the number of bytes required (by the method).
     * @param allocatedBytes the number of bytes the invoker expected to allocate
     * @param requiredBytes the number of bytes a method required
     */
    private WrongByteAllocationException(int allocatedBytes, int requiredBytes) {
        super(String.format("Caller expected %s byte%s, but %s required",
                allocatedBytes, allocatedBytes == 1 ? "s" : "", requiredBytes));
    }
}
