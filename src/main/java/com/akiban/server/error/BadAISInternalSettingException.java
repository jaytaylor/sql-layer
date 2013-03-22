
package com.akiban.server.error;

public class BadAISInternalSettingException extends InvalidOperationException {
    public BadAISInternalSettingException (String object, String name, String setting) {
        super (ErrorCode.BAD_INTERNAL_SETTING, object, name, setting);
    }
}
