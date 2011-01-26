package com.akiban.cserver.api;

public final class HapiRequestException extends  Exception {
    public enum ReasonCode {
        UNKNOWN(-1),
        UNPARSABLE(0),
        UNKNOWN_IDENTIFIER(1),
        MULTIBRANCH(2),
        PREDICATE_IS_PARENT(3),
        EXCEPTION_THROWN(4),
        ;

        private final int code;

        ReasonCode(int code) {
            this.code = code;
        }
    }

    private final ReasonCode reasonCode;

    private HapiRequestException(String message, ReasonCode reasonCode) {
        super(message);
        this.reasonCode = reasonCode == null ? ReasonCode.UNKNOWN : reasonCode;
    }

    private HapiRequestException(String message, Exception cause) {
        super(message, cause);
        this.reasonCode = ReasonCode.EXCEPTION_THROWN;
    }

    @Override
    public String getMessage() {
        return "<" + reasonCode + ">: " + super.getMessage();
    }

    public ReasonCode getReasonCode() {
        return reasonCode;
    }
}
