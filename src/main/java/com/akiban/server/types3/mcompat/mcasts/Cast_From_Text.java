
package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.common.types.TString;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

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
            this.fixedLength = targetClass.getFixedLength();
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            String in = source.getString();
            int maxLen = (fixedLength >= 0)
                    ? fixedLength
                    : context.inputTInstanceAt(0).attribute(StringAttribute.MAX_LENGTH);
            if (in.length() > maxLen) {
                String truncated = in.substring(0, maxLen);
                context.reportTruncate(in, truncated);
                in = truncated;
            }
            target.putString(in, null);
        }

        private final int fixedLength;
    }

    private Cast_From_Text() {}
}
