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

package com.akiban.server.test.mt.mtatomics;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.test.it.multiscan_update.MultiScanUpdateIT;

import java.util.List;

public final class MultiScanUpdateMT extends MultiScanUpdateIT {
    public MultiScanUpdateMT(TestMode testMode, WhichIndex scanIndex, WhichIndex updateColumn) {
        super(testMode, scanIndex, updateColumn);
    }


    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        return params(TestMode.MT);
    }
}
