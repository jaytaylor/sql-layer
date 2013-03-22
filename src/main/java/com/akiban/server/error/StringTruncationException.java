
package com.akiban.server.error;

public class StringTruncationException extends InvalidOperationException
{
    public StringTruncationException (String raw, String truncated)
    {
        super(ErrorCode.STRING_TRUNCATION, raw, truncated);
    }
}
