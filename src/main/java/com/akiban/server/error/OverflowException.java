package com.akiban.server.error;

public class OverflowException extends InvalidOperationException
{

        public OverflowException()
        {
            super(ErrorCode.OVERFLOW);
        }
}
