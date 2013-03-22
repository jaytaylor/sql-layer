
package com.akiban.server.types.conversion.util;

import com.akiban.server.types.ValueSource;

public interface AbstractConverter<T>
{
    /**
     * 
     * @param source
     * @return a (java) object that could be created
     *  using source's value
     */
     T get (ValueSource source);
     
     /**
      * 
      * @param string
      * @return a (java) object that could be created
      *  using string's value
      */
     T get (String string);
}