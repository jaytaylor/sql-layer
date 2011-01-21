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
