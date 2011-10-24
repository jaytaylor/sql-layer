package com.akiban.server.error;

public class DoubleOverflowException extends InvalidOperationException
{

        public DoubleOverflowException(String string)
        {
            super(ErrorCode.UNKNOWN, string); // ERROR CODE: TO BE REPLACED WITH SOMETHING ELSE
        }
}
