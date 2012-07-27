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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;


public class FunctionRegistryImpl implements FunctionRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(FunctionRegistryImpl.class);

    private final Set<Class<?>> searchClasses;
    
    private static final int SKIP = -1;
    private static final int FIELD = 0;
    private static final int ARRAY = 1;
    private static final int COLLECTION = 2;

    public FunctionRegistryImpl()
    throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
    {
        this(new ConfigurableClassFinder("t3s.txt"));
    }

    public FunctionRegistryImpl(ClassFinder classFinder)
    throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
    {
        // collect all scalar TOverload instances
        searchClasses = classFinder.findClasses();
    }

    @Override
    public <T> Collection<? extends T> find(Class<? extends T> targetClass) {
        return collectInstances(searchClasses, targetClass);
    }

    private static <T> Collection<T> collectInstances(Collection<Class<?>> classes, Class<T> target)
    {
        List<T> ret = new ArrayList<T>();
        for (Class<?> cls : classes) {
            if (!Modifier.isPublic(cls.getModifiers()))
                continue;
            else
                doCollecting(ret, cls, target);
        }
        return ret;
    }

    private static <T> void doCollecting(Collection<T> ret, Class<?> cls, Class<T> target) 
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
                            putItem(ret, field.get(null), target, field);
                            break;
                        case ARRAY:
                            for (Object item : (Object[])field.get(null)) {
                                if (target.isInstance(item))
                                    putItem(ret, item, target, field);
                            }
                            break;
                        case COLLECTION:
                            try
                            {
                                for (Object raw : (Collection<?>)field.get(null)) {
                                    if (target.isInstance(raw))
                                        putItem(ret, raw, target, field);
                                }
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
                            putItem(ret, method.invoke(null), target, method);
                            break;
                        case ARRAY:
                            for (Object item : (Object[])method.invoke(null))
                                putItem(ret, item, target, method);
                            break;
                        case COLLECTION:
                            try
                            {
                                for (Object raw : (Collection<?>)method.invoke(null))
                                    putItem(ret, raw, target, method);
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
            return assignable(method.getReturnType(), target, method.getGenericReturnType());
        return SKIP;
    }
    
    private static <T> int validateField(Field field, Class<T> target)
    { 
        int modifiers = field.getModifiers();
        
        if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers))
            return assignable(field.getType(), target, field.getGenericType());
         return SKIP;
    }

    private static <T> int assignable (Class<?> c, Class<T> target, Type genericTarget)
    {
        if (c.isArray() && target.isAssignableFrom(c.getComponentType())) {
            return ARRAY;
        }
        else if (target.isAssignableFrom(c)) {
            return FIELD;
        }
        else if (Collection.class.isAssignableFrom(c)) {
            if (genericTarget instanceof ParameterizedType) {
                ParameterizedType targetParams = (ParameterizedType) genericTarget;
                Type[] genericArgs = targetParams.getActualTypeArguments();
                assert genericArgs.length == 1 : Arrays.toString(genericArgs);
                Type genericArg = genericArgs[0];
                if (genericArg instanceof WildcardType) {
                    Type[] upperBounds = ((WildcardType)genericArg).getUpperBounds();
                    if (upperBounds.length > 1)
                        logger.debug("multiple upper bounds for {}: {}", genericTarget, Arrays.toString(upperBounds));
                    for (Type upperBound : upperBounds) {
                        if (isAssignableFrom(target, upperBound))
                            return COLLECTION;
                    }
                }
                else if (isAssignableFrom(target, genericArg))
                    return COLLECTION;
            }
        }
        return SKIP;
    }

    private static boolean isAssignableFrom(Class<?> target, Type actualType) {
        return (actualType instanceof Class<?>) && target.isAssignableFrom((Class<?>) actualType);
    }

    private static <T> void putItem(Collection<T> list, Object item,  Class<T> targetClass, Object source)
    {
        T cast;
        try {
            cast = targetClass.cast(item);
        } catch (ClassCastException e) {
            String err = "while casting " + item + " from " + source + " to " + targetClass;
            logger.error(err, e);
            throw new ClassCastException(err);
        }
        list.add(cast);
    }

    private static boolean isRegistered(AccessibleObject field)
    {
        return field.getAnnotation(DontRegister.class) == null;
    }
}
