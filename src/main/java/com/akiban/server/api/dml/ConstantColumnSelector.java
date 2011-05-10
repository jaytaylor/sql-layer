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

package com.akiban.server.api.dml;

public final class ConstantColumnSelector implements ColumnSelector {

    public static final ColumnSelector ALL_ON = new ConstantColumnSelector(true);
    public static final ColumnSelector ALL_OFF = new ConstantColumnSelector(false);

    private final boolean value;

    private ConstantColumnSelector(boolean value) {
        this.value = value;
    }

    @Override
    public boolean includesColumn(int columnPosition) {
        return value;
    }
}
