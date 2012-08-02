/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
                    : context.inputTInstanceAt(0).attribute(StringAttribute.LENGTH);
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
