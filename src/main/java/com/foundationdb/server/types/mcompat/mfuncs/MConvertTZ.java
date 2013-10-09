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

package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.pvalue.PValueSource;
import com.foundationdb.server.types.pvalue.PValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static com.foundationdb.server.types.mcompat.mtypes.MDatetimes.*;

public class MConvertTZ extends TScalarBase
{
    public static final TScalar INSTANCE = new MConvertTZ();

    private MConvertTZ() {}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MDatetimes.DATETIME, 0).covers(MString.VARCHAR, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        long ymd[] = MDatetimes.decodeDatetime(inputs.get(0).getInt64());
        
        if (ymd[MDatetimes.YEAR_INDEX] == 0 
                || ymd[MDatetimes.MONTH_INDEX] == 0
                || ymd[MDatetimes.DAY_INDEX] == 0)
            output.putNull();
        else
        {
            DateTimeZone fromTz;
            DateTimeZone toTz;
            try {
                fromTz = adjustTz(inputs.get(1).getString());
                toTz = adjustTz(inputs.get(2).getString());
            }
            catch (IllegalArgumentException e) {
                output.putNull();
                return;
            }
            DateTime date = new DateTime((int) ymd[YEAR_INDEX],
                                         (int) ymd[MONTH_INDEX],
                                         (int) ymd[DAY_INDEX],
                                         (int) ymd[HOUR_INDEX],
                                         (int) ymd[MIN_INDEX],
                                         (int) ymd[SEC_INDEX],
                                         0, // a DATETIME is only accurate up to SECOND,
                                         fromTz); // thus the MILLIS SEC field is 0
            
            output.putInt64(encodeDatetime(date.withZone(toTz)));
        }
    }

    @Override
    public String displayName()
    {
        return "CONVERT_TZ";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MDatetimes.DATETIME);
    }
    
    /**
     * joda's non-named timezone  (off-set hour) is in the form:
     * [<PLUS>] | <MINUS>]<NUMBER><NUMBER><COLON><NUMBER><NUMBER>
     * 
     * 1 digit number is not use.
     * 
     * - This prepend 0 to make it 2 digit number
     * - Named timezones are also 'normalised' 
     * @param st
     * @return 
     */
    private static DateTimeZone adjustTz(String st)
    {
        for ( int n = 0; n < st.length(); ++n)
        {
            char ch;
            if ((ch = st.charAt(n)) == ':')
            {
                int index = n - 2; // if the character that is 2 chars to the left of the COLON
                if (index < 0 )    //  is not a digit, then we need to pad a '0' there
                    return DateTimeZone.forID(st);
                ch = st.charAt(index);
                if (ch == '-' || ch == '+')
                {
                    StringBuilder bd = new StringBuilder(st);
                    bd.insert(1, '0');
                    return DateTimeZone.forID(bd.toString());
                }
                break;
            }
            else if (ch == '/')
                return DateTimeZone.forID(st);
        }
        return DateTimeZone.forID(st.toUpperCase());
    }
}
