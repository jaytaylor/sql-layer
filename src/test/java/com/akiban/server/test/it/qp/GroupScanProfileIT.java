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

package com.akiban.server.test.it.qp;

import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.physicaloperator.UndefBindings;
import org.junit.Ignore;
import org.junit.Test;

import static com.akiban.qp.physicaloperator.API.cursor;
import static com.akiban.qp.physicaloperator.API.groupScan_Default;

public class GroupScanProfileIT extends QPProfileITBase
{
    @Test
    @Ignore
    public void profileGroupScan()
    {
        final int SCANS = 1000;
        final int CUSTOMERS = 1000;
        final int ORDERS_PER_CUSTOMER = 5;
        final int ITEMS_PER_ORDER = 2;
        populateDB(CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER);
        long start = System.nanoTime();
        PhysicalOperator plan = groupScan_Default(coi);
        for (int s = 0; s < SCANS; s++) {
            Cursor cursor = cursor(plan, adapter);
            cursor.open(NO_BINDINGS);
            while (cursor.next() != null) {
            }
            cursor.close();
        }
        long end = System.nanoTime();
        double sec = (end - start) / (1000.0 * 1000 * 1000);
        System.out.println(String.format("scans: %s, db: %s/%s/%s, time: %s",
                                         SCANS, CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER, sec));
    }

    private static final Bindings NO_BINDINGS = UndefBindings.only();
}
