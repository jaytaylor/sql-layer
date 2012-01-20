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

package com.akiban.qp.operator;

import com.akiban.util.Tap;

class TopLevelWrappingCursor extends ChainedCursor {

    // Cursor interface

    @Override
    public void open() {
        try {
            CURSOR_SETUP_TAP.in();
            super.open();
            CURSOR_SETUP_TAP.out();
            CURSOR_SCAN_TAP.in();
        } catch (RuntimeException e) {
            throw launder(e);
        }
    }

    @Override
    public void close() {
        try {
            super.close();
            CURSOR_SCAN_TAP.out();
        } catch (RuntimeException e) {
            throw launder(e);
        }
    }

    // WrappingCursor interface

    TopLevelWrappingCursor(QueryContext context, Cursor input) {
        super(context, input);
    }

    // private methods

    private static RuntimeException launder(RuntimeException exception) {
        return exception;
    }

    // Class state

    private static final Tap.InOutTap CURSOR_SETUP_TAP = Tap.createTimer("cursor setup");
    private static final Tap.InOutTap CURSOR_SCAN_TAP = Tap.createTimer("cursor scan");

}
