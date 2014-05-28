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

package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.util.Strings;

@SuppressWarnings("unused")
public final class Cast_From_Text {

    public static final TCast LONGTEXT_TO_MEDIUMTEXT = new TextCast(MString.LONGTEXT, MString.MEDIUMTEXT);
    public static final TCast LONGTEXT_TO_TEXT = new TextCast(MString.LONGTEXT, MString.TEXT);
    public static final TCast LONGTEXT_TO_TINYTEXT = new TextCast(MString.LONGTEXT, MString.TINYTEXT);
    public static final TCast LONGTEXT_TO_CHAR = new TextCast(MString.LONGTEXT, MString.CHAR);
    public static final TCast LONGTEXT_TO_VARCHAR = new TextCast(MString.LONGTEXT, MString.VARCHAR);

    public static final TCast MEDIUMTEXT_TO_LONGTEXT = new TextCast(MString.MEDIUMTEXT, MString.LONGTEXT);
    public static final TCast MEDIUMTEXT_TO_TEXT = new TextCast(MString.MEDIUMTEXT, MString.TEXT);
    public static final TCast MEDIUMTEXT_TO_TINYTEXT = new TextCast(MString.MEDIUMTEXT, MString.TINYTEXT);
    public static final TCast MEDIUMTEXT_TO_CHAR = new TextCast(MString.MEDIUMTEXT, MString.CHAR);
    public static final TCast MEDIUMTEXT_TO_VARCHAR = new TextCast(MString.MEDIUMTEXT, MString.VARCHAR);

    public static final TCast TEXT_TO_LONGTEXT = new TextCast(MString.TEXT, MString.LONGTEXT);
    public static final TCast TEXT_TO_MEDIUMTEXT = new TextCast(MString.TEXT, MString.MEDIUMTEXT);
    public static final TCast TEXT_TO_TINYTEXT = new TextCast(MString.TEXT, MString.TINYTEXT);
    public static final TCast TEXT_TO_CHAR = new TextCast(MString.TEXT, MString.CHAR);
    public static final TCast TEXT_TO_VARCHAR = new TextCast(MString.TEXT, MString.VARCHAR);

    public static final TCast TINYTEXT_TO_LONGTEXT = new TextCast(MString.TINYTEXT, MString.LONGTEXT);
    public static final TCast TINYTEXT_TO_MEDIUMTEXT = new TextCast(MString.TINYTEXT, MString.MEDIUMTEXT);
    public static final TCast TINYTEXT_TO_TEXT = new TextCast(MString.TINYTEXT, MString.TEXT);
    public static final TCast TINYTEXT_TO_CHAR = new TextCast(MString.TINYTEXT, MString.CHAR);
    public static final TCast TINYTEXT_TO_VARCHAR = new TextCast(MString.TINYTEXT, MString.VARCHAR);

    public static final TCast CHAR_TO_LONGTEXT = new TextCast(MString.CHAR, MString.LONGTEXT);
    public static final TCast CHAR_TO_MEDIUMTEXT = new TextCast(MString.CHAR, MString.MEDIUMTEXT);
    public static final TCast CHAR_TO_TEXT = new TextCast(MString.CHAR, MString.TEXT);
    public static final TCast CHAR_TO_TINYTEXT = new TextCast(MString.CHAR, MString.TINYTEXT);
    public static final TCast CHAR_TO_VARCHAR = new TextCast(MString.CHAR, MString.VARCHAR);

    public static final TCast VARCHAR_TO_LONGTEXT = new TextCast(MString.VARCHAR, MString.LONGTEXT);
    public static final TCast VARCHAR_TO_MEDIUMTEXT = new TextCast(MString.VARCHAR, MString.MEDIUMTEXT);
    public static final TCast VARCHAR_TO_TEXT = new TextCast(MString.VARCHAR, MString.TEXT);
    public static final TCast VARCHAR_TO_TINYTEXT = new TextCast(MString.VARCHAR, MString.TINYTEXT);
    public static final TCast VARCHAR_TO_CHAR = new TextCast(MString.VARCHAR, MString.CHAR);

    private static class TextCast extends TCastBase {
        private TextCast(TString sourceClass, TString targetClass) {
            super(sourceClass, targetClass);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            String in = source.getString();
            TInstance outputType = context.outputType();
            int maxLen = outputType.attribute(StringAttribute.MAX_LENGTH);
            String truncated = Strings.truncateIfNecessary(in, maxLen);
            if (in != truncated) {
                context.reportTruncate(in, truncated);
                in = truncated;
            }
            target.putString(in, TString.getCollator(outputType));
        }
    
        @Override
        public TInstance preferredTarget(TPreptimeValue source) {
            TInstance sourceType =  source.type();
            return targetClass().instance(sourceType.attribute(StringAttribute.MAX_LENGTH),
                    sourceType.attribute(StringAttribute.CHARSET),
                    sourceType.attribute(StringAttribute.COLLATION),
                    source.isNullable());
        }
    }

    private Cast_From_Text() {}
}
