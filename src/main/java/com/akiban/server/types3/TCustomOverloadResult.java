
package com.akiban.server.types3;

import java.util.List;

public interface TCustomOverloadResult {
    TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context);
}
