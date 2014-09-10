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

import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.sql.LayerVersionInfo;
import com.foundationdb.sql.Main;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.std.NoArgExpression;
import java.util.Date;

public class NoArgFuncs
{
    static final int USER_NAME_LENGTH = 77;
    static final int SCHEMA_NAME_LENGTH = 128;

    protected static String buildVersion(LayerVersionInfo vinfo) {
        StringBuilder version = new StringBuilder("FoundationDB ");
        version.append(vinfo.versionShort);
        version.append(" ");
        int idx = vinfo.versionShort.length();
        if (vinfo.versionLong.length() > idx) {
            if (vinfo.versionLong.charAt(idx) == '-') idx++;
            version.append(vinfo.versionLong, idx, vinfo.versionLong.length());
        }
        return version.toString();
    }

    public static final TScalar SHORT_SERVER_VERSION = new NoArgExpression("version", true)
    {
        private final String VERSION_STR = buildVersion(Main.VERSION_INFO);

        @Override
        public void evaluate(TExecutionContext context, ValueTarget target)
        {
            target.putString(VERSION_STR, null);
        }

        @Override
        protected TClass resultTClass() {
            return MString.VARCHAR;
        }

        @Override
        protected int[] resultAttrs() {
            return new int[]{VERSION_STR.length()};
        }

        @Override
        protected boolean neverConstant() {
            return false;
        }

    };
    
    public static final TScalar PI = new TScalarBase()
    {
        @Override
        protected void buildInputSets(TInputSetBuilder builder)
        {
            // does nothing. doesn't take any arg
        }

        
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
        {
            output.putDouble(Math.PI);
        }

        @Override
        public String displayName()
        {
            return "PI";
        }

        @Override
        public TOverloadResult resultType()
        {
            return TOverloadResult.fixed(MApproximateNumber.DOUBLE);
        }

        @Override
        protected boolean neverConstant() {
            return false;
        }
    };
 
    public static final TScalar CUR_DATE = new NoArgExpression("CURRENT_DATE", true)
    {
        @Override
        public String[] registeredNames()
        {
            return new String[] {"curdate", "current_date"};
        }
        
        @Override
        public TClass resultTClass()
        {
            return MDateAndTime.DATE;
        }

        @Override
        public void evaluate(TExecutionContext context, ValueTarget target)
        {
            target.putInt32(MDateAndTime.encodeDate(context.getCurrentDate(), context.getCurrentTimezone()));
        }
    };

    public static final TScalar CUR_TIME = new NoArgExpression("CURRENT_TIME", true)
    {
        @Override
        public String[] registeredNames()
        {
            return new String[] {"curtime", "current_time"};
        }
        
        @Override
        public TClass resultTClass()
        {
            return MDateAndTime.TIME;
        }

        @Override
        public void evaluate(TExecutionContext context, ValueTarget target)
        {
            target.putInt32(MDateAndTime.encodeTime(context.getCurrentDate(), context.getCurrentTimezone()));
        }
    };

    public static final TScalar CUR_TIMESTAMP = new NoArgExpression("CURRENT_TIMESTAMP", true)
    {
        @Override
        public String[] registeredNames()
        {
            return new String[] {"current_timestamp", "now", "localtime", "localtimestamp"};
        }

        @Override
        public TClass resultTClass()
        {
            return MDateAndTime.DATETIME;
        }

        @Override
        public void evaluate(TExecutionContext context, ValueTarget target)
        {
            target.putInt64(MDateAndTime.encodeDateTime(context.getCurrentDate(), context.getCurrentTimezone()));
        }
    };
    
    public static final TScalar UNIX_TIMESTAMP = new NoArgExpression("UNIX_TIMESTAMP", true)
    {
        @Override
        public void evaluate(TExecutionContext context, ValueTarget target)
        {
            target.putInt32((int)MDateAndTime.encodeTimestamp(context.getCurrentDate(), context));
        }

        @Override
        protected TClass resultTClass()
        {
            return MDateAndTime.TIMESTAMP;
        }

    };
    
    public static final TScalar SYSDATE = new NoArgExpression("SYSDATE", false)
    {
        @Override
        public TClass resultTClass()
        {
            return MDateAndTime.DATETIME;
        }

        @Override
        public void evaluate(TExecutionContext context, ValueTarget target)
        {
            target.putInt64(MDateAndTime.encodeDateTime(new Date().getTime(), context.getCurrentTimezone()));
        }
    };

    public static final TScalar CURRENT_USER = new NoArgExpression("CURRENT_USER", true) 
    {
        @Override
        public TClass resultTClass() {
            return MString.VARCHAR;
        }

        @Override
        protected int[] resultAttrs() {
            return new int[] { USER_NAME_LENGTH };
        }

        @Override
        public void evaluate(TExecutionContext context, ValueTarget target) {
            target.putString(context.getCurrentUser(), null);
        }
    };

    public static final TScalar SESSION_USER = new NoArgExpression("SESSION_USER", true)
    {
        @Override
        public TClass resultTClass() {
            return MString.VARCHAR;
        }

        @Override
        protected int[] resultAttrs() {
            return new int[] { USER_NAME_LENGTH };
        }

        @Override
        public void evaluate(TExecutionContext context, ValueTarget target)
        {
            target.putString(context.getSessionUser(), null);
        }

    };
    
    public static final TScalar SYSTEM_USER = new NoArgExpression("SYSTEM_USER", true)
    {
        @Override
        public TClass resultTClass() {
            return MString.VARCHAR;
        }

        @Override
        protected int[] resultAttrs() {
            return new int[] { USER_NAME_LENGTH };
        }

        @Override
        public void evaluate(TExecutionContext context, ValueTarget target)
        {
            target.putString(context.getSystemUser(), null);
        }

        @Override
        protected boolean neverConstant() {
            return false;
        }
    };
    
    public static final TScalar CURRENT_SCHEMA = new NoArgExpression("CURRENT_SCHEMA", true)
    {
        @Override
        public TClass resultTClass() {
            return MString.VARCHAR;
        }

        @Override
        protected int[] resultAttrs() {
            return new int[] { SCHEMA_NAME_LENGTH };
        }

        @Override
        public void evaluate(TExecutionContext context, ValueTarget target)
        {
            target.putString(context.getCurrentSchema(), null);
        }
    };
    
    public static final TScalar CURRENT_SESSION_ID = new NoArgExpression("CURRENT_SESSION_ID", true)
    {
        @Override
        public TClass resultTClass() {
            return MNumeric.INT;
        }

        @Override
        public void evaluate(TExecutionContext context, ValueTarget target)
        {
            target.putInt32(context.getSessionId());
        }
    };
}
