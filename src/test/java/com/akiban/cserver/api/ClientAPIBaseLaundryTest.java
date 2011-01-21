package com.akiban.cserver.api;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.message.ErrorCode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public final class ClientAPIBaseLaundryTest {
    private static final ErrorCode[] IGNORED_CODES = {

            ErrorCode.MULTIGENERATIONAL_TABLE, // shouldn't (yet!) be possible to even get to this

            ErrorCode.UNKNOWN, // generic case, so we should throw a generic error
            ErrorCode.INTERNAL_ERROR, // generic case, so we should throw a generic error
            ErrorCode.INTERNAL_CORRUPTION, // generic case, so we should throw a generic error
            ErrorCode.UNEXPECTED_EXCEPTION, // generic case, so we should throw a generic error
            ErrorCode.UNSUPPORTED_OPERATION, // generic case, so we should throw a generic error

            ErrorCode.SERVER_SHUTDOWN, // shouldn't happen from D*LFunctions layer
            ErrorCode.STALE_AIS, // shouldn't happen from D*LFunctions layer
            ErrorCode.MALFORMED_REQUEST, // shouldn't happen from D*LFunctions layer
            ErrorCode.TABLEDEF_MISMATCH, // D*LFunctions layer will throw TableDefinitionMismatchException directly
            ErrorCode.ROW_OUTPUT, // D*LFunctions layer will throw RowOutputException directly

            ErrorCode.NO_REFERENCED_ROW, // TODO: not sure what this means!
    };

    @Parameterized.Parameters
    public static List<Object[]> data() {
        Set<ErrorCode> ignore = new HashSet<ErrorCode>(Arrays.asList(IGNORED_CODES));

        List<Object[]> params = new ArrayList<Object[]>();
        for (ErrorCode errorCode : ErrorCode.values()) {
            if (!ignore.contains(errorCode)) {
                params.add( new Object[]{errorCode});
            }
        }
        return params;
    }

    private final ErrorCode errorCode;

    public ClientAPIBaseLaundryTest(ErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    @Test
    public void isLaundered() {
        InvalidOperationException ioeBasic = new InvalidOperationException(errorCode, "my message");
        InvalidOperationException ioeLaundered = ClientAPIBase.launder(ioeBasic);
        
        if(ioeLaundered.getClass().equals(InvalidOperationException.class)) {
//            System.err.println(errorCode);
            fail(errorCode.name());
        }
    }
}
