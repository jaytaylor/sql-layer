
package com.akiban.server.store.statistics.histograms;

import java.util.List;

public interface Splitter<T> {
    int segments();
    List<? extends T> split(T input);
}
