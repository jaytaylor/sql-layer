
package com.akiban.server.types3.common;

import com.akiban.server.types3.TClassFormatter;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.AkibanAppender;

public class TFormatter {

    public static enum FORMAT implements TClassFormatter {
        BOOL {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(Boolean.toString(source.getBoolean()));
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(source.getBoolean() ? "TRUE" : "FALSE");
            }

            @Override
            public void formatAsJson(TInstance instance, PValueSource source, AkibanAppender out) {
                format(instance, source, out);
            }
        }
    }
}
