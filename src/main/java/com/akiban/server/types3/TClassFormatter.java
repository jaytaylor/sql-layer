package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.AkibanAppender;

public interface TClassFormatter {
    /** Format value in <code>source</code> in a type-specific way. */
    public void format(TInstance instance, PValueSource source, AkibanAppender out);
    /** Format value in <code>source</code> as a SQL literal. */
    public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out);
    /** Format value in <code>source</code> as a JSON value, including any necessary quotes. */
    public void formatAsJson(TInstance instance, PValueSource source, AkibanAppender out);
}
