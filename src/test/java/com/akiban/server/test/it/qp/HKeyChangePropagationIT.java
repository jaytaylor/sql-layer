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

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.operator.*;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.util.Tap;
import com.akiban.util.TapReport;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

import static com.akiban.qp.operator.API.*;

public class HKeyChangePropagationIT extends QPProfileITBase
{
    @Test
    @Ignore
    public void profileHKeyChangePropagation() throws PersistitException
    {
        final int SCANS = 100; // Number of times to toggle each customer.cid
        final int CUSTOMERS = 100;
        final int ORDERS_PER_CUSTOMER = 20;
        final int ITEMS_PER_ORDER = 20;
        populateDB(CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER);
        Operator scanPlan =
            filter_Default(
                groupScan_Default(coi),
                Collections.singleton(customerRowType));
        UpdatePlannable updatePlan =
        update_Default(scanPlan,
                       new UpdateFunction()
                       {
                           @Override
                           public Row evaluate(Row original, Bindings bindings)
                           {
                               OverlayingRow updatedRow = new OverlayingRow(original);
                               updatedRow.overlay(0, original.eval(0).getInt() - 1000000);
                               return updatedRow;
                           }

                           @Override
                           public boolean rowIsSelected(Row row)
                           {
                               return true;
                           }
                       });
        Transaction transaction = treeService().getTransaction(session());
        transaction.begin();
        Tap.setEnabled(".*propagate.*", true);
        long start = System.nanoTime();
        for (int s = 0; s < SCANS; s++) {
            updatePlan.run(NO_BINDINGS, adapter);
        }
        long end = System.nanoTime();
        transaction.commit();
        transaction.end();
        double sec = (end - start) / (1000.0 * 1000 * 1000);
        System.out.println(String.format("scans: %s, db: %s/%s/%s, time: %s",
                                         SCANS, CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER, sec));
        TapReport propagateReport = Tap.getReport(".*propagate.*")[0];
        System.out.println(String.format("propagations: %s", propagateReport.getInCount()));
    }

    private static final Bindings NO_BINDINGS = UndefBindings.only();
}
