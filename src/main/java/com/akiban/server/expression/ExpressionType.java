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

package com.akiban.server.expression;

import com.akiban.server.types.AkType;

public interface ExpressionType {
    /**
     * The type represented by this expression. {@code this.evaluation().eval()} returns a {@link ValueSource} whose
     * {@link ValueSource#getConversionType()} method must return the same type as returned by this method (or NULL).
     * @return the AkType this expression's runtime instance will eventually have
     */
    AkType getType();

    /**
     * The precision of the value that will be returned.  For string
     * types, this is the maximum number of characters.  For decimal
     * numbers, it is the actual precision.  For most other types, or
     * if very difficult to compute, return <code>0</code>.
     */
    int getPrecision();

    /**
     * The scale of the value that will be returned.
     * Only meaningful for decimal numeric types.
     * Others return <code>0</code>.
     */
    int getScale();
}
