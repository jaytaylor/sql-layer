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

package com.akiban.server.types3.service;

import com.akiban.server.service.functions.FunctionsRegistryImpl.FunctionsRegistryException;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TOverload;
import com.google.inject.Singleton;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Singleton
public class FunctionRegistryImpl implements FunctionRegistry
{
    // TODO : define aggregates here
    private List<TOverload> scalars;
    private List<TClass> types;
    private List<TCast> casts;
    
    private static final int SKIP = -1;
    private static final int FIELD = 0;
    private static final int ARRAY = 1;
    private static final int COLLECTION = 2;

    FunctionRegistryImpl (FunctionsClassFinder finder) 
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
    {
        scalars = new ArrayList<TOverload>();
        types = new ArrayList<TClass>();
        casts = new ArrayList<TCast>();
        
        for (Class<?> cls : finder.findClasses())
        {
            if (!Modifier.isPublic(cls.getModifiers()))
                continue;
            collectInstances(scalars, cls, TOverload.class);
            collectInstances(types, cls, TClass.class);
            collectInstances(casts, cls, TCast.class);
        }
    }

    private static <T> void collectInstances(Collection<T> ret, Class<?> cls, Class<T> target) 
    {
        try
        {
            // grab the static INSTANCEs fields
            for (Field field : cls.getFields())
            {
                if (isRegistered(field))
                    switch(validateField(field, target))
                    {
                        case FIELD:
                            putItem(ret, (T)field.get(null));
                            break;
                        case ARRAY:
                            for (T item : (T[])field.get(null))
                                putItem(ret, (T)item);
                            break;
                        case COLLECTION:
                            try
                            {
                                for (Object raw : (Collection<?>)field.get(null))
                                    putItem(ret, (T)raw);
                                break;
                            }
                            catch (ClassCastException e) {/* fall thru */}
                        default:
                               // SKIP (does nothing)
                    }
            }
            
            // grab the static methods that create instances
            for (Method method : cls.getMethods())
            {
                
                if (isRegistered(method))
                    switch(validateMethod(method, target))
                    {
                        case FIELD:
                            putItem(ret, (T)method.invoke(null));
                            break;
                        case ARRAY:
                            for (T item : (T[])method.invoke(null))
                                putItem(ret, item);
                            break;
                        case COLLECTION:
                            try
                            {
                                for (Object raw : (Collection<?>)method.invoke(null))
                                    putItem(ret, (T)raw);
                                break;
                            }
                            catch (ClassCastException e) {/* fall thru */}
                        default:
                            // SKIP (does nothing)
                    }
            }
           
        }
        catch (IllegalAccessException e)   
        {
            throw new FunctionsRegistryException(e.getMessage());
        }
        catch (InvocationTargetException ex)
        {
            throw new FunctionsRegistryException(ex.getMessage());
        }
    }

    private static <T> int validateMethod(Method method, Class<T> target)
    {
        int modifiers = method.getModifiers();
        if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers)
                && method.getParameterTypes().length == 0)
            return assignable(method.getReturnType(), target);
        return SKIP;
    }
    
    private static <T> int validateField(Field field, Class<T> target)
    { 
        int modifiers = field.getModifiers();
        
        if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers))
            return assignable(field.getType(), target);
         return SKIP;
    }

    private static <T> int assignable (Class<?> c, Class<T> target)
    {
        if (c.isArray() && target.isAssignableFrom(c.getComponentType()))
            return ARRAY;
        else if (TOverload.class.isAssignableFrom(c))
            return FIELD;
        else return COLLECTION;
    }

    
    private static <T> void putItem(Collection<T> list, T item)
    {
        list.add(item);
    }
    
    private static boolean isRegistered(AccessibleObject field)
    {
        return field.getAnnotation(DontRegister.class) == null;
    }
    
    @Override
    public FunctionKind getFunctionKind(String name)
    {
        throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public Collection<TOverload> overloads()
    {
        return scalars;
    }
    
    @Override
    public Collection<TCast> casts()
    {
        return casts;
    }
    
    @Override
    public Collection<TClass> tclasses()
    {
        return types;
    }
}
