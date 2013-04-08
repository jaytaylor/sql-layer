/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.service.text.FullTextIndexService;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;


public class FulltextMaintenanceWait extends TScalarBase
{

    public static final TScalar INSTANCE = new FulltextMaintenanceWait();
    
    private FulltextMaintenanceWait() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        // no argument
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        QueryContext qc = context.getQueryContext();
        if (qc == null)
        {
            context.logError ("No querycontext for retreiving full-text service");
            output.putInt32(-1);
        }
        else
        {
            FullTextIndexService service = qc.getServiceManager().getServiceByClass(FullTextIndexService.class);
            try
            {
                WaitFunctionHelpers.waitOn(service.getBackgroundWorks());
                output.putInt32(0);
            }
            catch (InterruptedException ex)
            {
                context.logError(ex.getMessage());
                output.putInt32(-2);
            }
        }
    }

    @Override
    public String displayName()
    {
        return "fulltext_maintenance_wait";
    }

    @Override
    public TOverloadResult resultType()
    {
        // TODO: return value probably doesn't mean anything! to anyone not having
        // access to the code.
        // Maybe return a STRING/msg instead?
        return TOverloadResult.fixed(MNumeric.INT);
    }
}
