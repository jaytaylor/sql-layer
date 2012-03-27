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

package com.akiban.server.api;

public final class HapiRequestException extends  Exception {
    public enum ReasonCode {
        UNKNOWN(-1),
        UNPARSABLE(0),
        UNKNOWN_IDENTIFIER(1),
        MULTIBRANCH(2),
        UNSUPPORTED_REQUEST(3),
        EXCEPTION_THROWN(4),
        WRITE_ERROR(5),
        INTERNAL_ERROR(6)
        ;

        private final int code;

        ReasonCode(int code) {
            this.code = code;
        }

        public boolean warrantsErrorLogging() {
            return this.equals(INTERNAL_ERROR) || this.equals(EXCEPTION_THROWN);
        }
    }

    private final ReasonCode reasonCode;
    private final String message;

    public HapiRequestException(String message, ReasonCode reasonCode) {
        super(message);
        this.reasonCode = reasonCode == null ? ReasonCode.UNKNOWN : reasonCode;
        this.message = message;
    }

    public HapiRequestException(String message, Exception cause) {
        super(message, cause);
        this.reasonCode = ReasonCode.EXCEPTION_THROWN;
        this.message = message;
    }

    public HapiRequestException(String message, Throwable cause, ReasonCode reasonCode) {
        super(message, cause);
        this.reasonCode = reasonCode == null ? ReasonCode.UNKNOWN : reasonCode;
        this.message = message;
    }

    @Override
    public String getMessage() {
        return "<" + reasonCode + ">: " + super.getMessage();
    }

    public String getSimpleMessage() {
        return message;
    }

    public ReasonCode getReasonCode() {
        return reasonCode;
    }
}
