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
package com.akiban.direct;

import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;

/**
 * ClassLoader that delegates selectively. This loader is used to
 * 
 * @author peter
 * 
 */
public class DirectClassLoader extends ClassLoader {

    final ClassLoader parent;

    private final static String[] DIRECT_INTERFACES = { "com.akiban.DirectContext", "com.akiban.DirectModule",
            "com.akiban.DaoPrototype" };

    public DirectClassLoader(ClassLoader bootstrap, ClassLoader parent) {
        super(bootstrap);
        this.parent = parent;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return loadClass(name, false);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        /*
         * Not a parallel ClassLoader until necessary
         */
        synchronized (this) {
            Class<?> cl = findLoadedClass(name);
            if (cl == null) {
                for (final String special : DIRECT_INTERFACES) {
                    if (special.equals(name)) {
                        cl = parent.loadClass(name);
                        if (cl == null) {
                            throw new ClassNotFoundException(name);
                        }
                        break;
                    }
                }
            }
            if (cl == null) {
                cl = findClass(name);
            }
            if (cl == null) {
                cl = compileSpecialClass(name);
            }
            if (cl == null) {
                throw new ClassNotFoundException(name);
            }
            if (resolve) {
                resolveClass(cl);
            }
            return cl;
        }
    }

    private Class<?> compileSpecialClass(final String name) throws ClassNotFoundException {
        throw new ClassNotFoundException(name);
    }
    
    
    public Class<? extends DirectModule> loadModule(AkibanInformationSchema ais, String moduleName, List<String> urls) throws Exception {
        throw new UnsupportedOperationException("hi there");
    }
}
