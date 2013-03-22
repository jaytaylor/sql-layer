
package com.akiban.server.types3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A very thin shim around List<TClass></TClass>. Mostly there so that the call sites don't have to worry about
 * generics. This is especially useful for the reflective registration, where it's easier to search for a TCastPath
 * than fora {@code Collection&lt;? extends List&lt;? extends TClass&gt;&gt;}.
 */
public final class TCastPath {

    public static TCastPath create(TClass first, TClass second, TClass third, TClass... rest) {
        TClass[] all = new TClass[rest.length + 3];
        all[0] = first;
        all[1] = second;
        all[2] = third;
        System.arraycopy(rest, 0, all, 3, rest.length);
        List<? extends TClass> list = Arrays.asList(all);
        return new TCastPath(list);
    }

    private TCastPath(List<? extends TClass> list) {
        if (list.size() < 3)
            throw new IllegalArgumentException("cast paths must contain at least three elements: " + list);
        this.list = Collections.unmodifiableList(list);
    }

    public List<? extends TClass> getPath() {
        return list;
    }

    private final List<? extends TClass> list;
}
