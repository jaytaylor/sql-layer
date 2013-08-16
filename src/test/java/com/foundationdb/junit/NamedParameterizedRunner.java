/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.junit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Ignore;
import org.junit.internal.builders.IgnoredClassRunner;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.regex.Pattern;

public final class NamedParameterizedRunner extends Suite
{
    /**
     * <p>Parameterization override filter (works by name).</p>
     *
     * <p>If this property is set, then only parameterization names that match its value will be processed. These
     * names behave like @Failing names (in terms of regexes, etc). If this property is set and a test that matches
     * it is marked as @Failing, that test will still get run. For instance, if a parameterization named
     * <tt>myFooTest</tt> is marked as failing for a given test (either because the entire parameterization is marked
     * as failing, or because of a <tt>@Failing</tt> annotation on the method), and if you have a system property
     * <tt>{@value} == "/myFoo/"</tt>, then the test <em>will</em> be run.
     */
    public final static String PARAMETERIZATION_OVERRIDE = "fdbsql.test.param.override";
	/**
	 * <p>Annotation for a method which provides parameters for an
     * {@link NamedParameterizedRunner} suite.</p>
     *
     * <p>A class that is run with {@linkplain NamedParameterizedRunner} <em>must</em> have exactly one method
     * marked with this annotation, and that method must:</p>
     * <ul>
     *  <li>be public</li>
     *  <li>be static</li>
     *  <li>take no arguments</li>
     *  <li>return Collection of {@link Parameterization} objects.</li>
     * </ul>
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	public static @interface TestParameters {
	}

    private final static Logger logger = LoggerFactory.getLogger(NamedParameterizedRunner.class);
    private final List<Runner> runners;

    /**
     * Adapted from {@link org.junit.runners.Parameterized}'s nested class
     * @see
     */
    static class ReifiedParamRunner extends BlockJUnit4ClassRunner
    {
        private final Parameterization parameterization;
        private final boolean overrideOn;
        private Object testObject = null;

        private static class OnlyIfErrorFrameworkMethod extends FrameworkMethod {
            private final OnlyIfException exception;

            private OnlyIfErrorFrameworkMethod(Method method, OnlyIfException exception) {
                super(method);
                this.exception = exception;
            }

            @Override
            public Object invokeExplosively(Object target, Object... params) throws Throwable {
                throw exception;
            }
        }

        public ReifiedParamRunner(Class<?> klass, Parameterization parameterization, boolean overrideOn)
                throws InitializationError
        {
            super(klass);
            this.parameterization = parameterization;
            this.overrideOn = overrideOn;
        }

        /**
         * For debugging.
         * @return parameterization.toString()
         * @throws NullPointerException if parameterization is null
         */
        String paramToString()
        {
            return parameterization.toString();
        }

        /**
         * For debugging
         * @return the override value
         */
        boolean overrideOn()
        {
            return overrideOn;
        }

        /**
         * For debugging.
         * @param name the name of the child FrameworkMethod to get
         * @return the child, or null if it doesn't exist
         */
        FrameworkMethod getChild(String name)
        {
            for (FrameworkMethod method : getChildren())
            {
                if (method.getMethod().toString().equals(name))
                {
                    return method;
                }
            }
            return null;
        }

        /**
         * For debugging
         * @return the number of children in this runner
         */
        int getChildrenCount() {
            return getChildren().size();
        }

        /**
         * For debugging (and only for assertion messages)
         * @return shows human-readable info about the children
         */
        String describeChildren() {
            List<FrameworkMethod> children = getChildren();
            if (children == null) {
                return "null";
            }
            List<String> descriptions = new ArrayList<>(children.size());
            for (FrameworkMethod method : children) {
                descriptions.add( method.getName() );
            }
            return descriptions.toString();
        }

        @Override
        public Object createTest() throws Exception
        {
            if (testObject != null) {
                return testObject;
            }
            try
            {
                testObject = getTestClass().getOnlyConstructor().newInstance(parameterization.getArguments());
                return testObject;
            }
            catch(IllegalArgumentException e)
            {
                throw new IllegalArgumentException("parameters: " + Arrays.toString(parameterization.getArguments()), e);
            }
        }

        @Override
        protected List<FrameworkMethod> getChildren() {
            List<FrameworkMethod> ret = super.getChildren();
            for(ListIterator<FrameworkMethod> iter = ret.listIterator(); iter.hasNext(); ) {
                FrameworkMethod frameworkMethod = iter.next();
                try {
                    if (!onlyIfFilter(frameworkMethod)) {
                        iter.remove();
                    }
                } catch (OnlyIfException e) {
                    iter.set(new OnlyIfErrorFrameworkMethod(frameworkMethod.getMethod(), e));
                }
            }
            return ret;
        }

        @Override
        public String getName()
        {
            return parameterization.getName();
        }

        @Override
        public String testName(FrameworkMethod method)
        {
            return String.format("%s [%s]", method.getName(), parameterization.getName());
        }

		@Override
		protected void validateConstructor(List<Throwable> errors)
        {
			validateOnlyOneConstructor(errors);
		}

		@Override
		protected Statement classBlock(RunNotifier notifier)
        {
			return childrenInvoker(notifier);
		}

        @Override
        protected void runChild(FrameworkMethod method, RunNotifier notifier)
        {
            if (expectedToPass(method))
            {
                super.runChild(method, notifier);
            }
            else
            {
                notifier.fireTestIgnored( describeChild(method));
            }
        }

        private static class OnlyIfException extends Exception {
            private OnlyIfException(String message, Throwable cause) {
                super(message, cause);
            }

            private OnlyIfException(String message) {
                super(message);
            }
        }

        boolean onlyIfFilter(FrameworkMethod method) throws OnlyIfException {
            OnlyIf onlyIf = method.getAnnotation(OnlyIf.class);
            if (onlyIf != null) {
                if (!runOnlyIf(onlyIf.value(), true)) {
                    return false;
                }
            }
            OnlyIfNot onlyIfNot = method.getAnnotation(OnlyIfNot.class);
            return onlyIfNot == null || runOnlyIf(onlyIfNot.value(), false);
        }

        private boolean runOnlyIf(String methodName, boolean mustEqual) throws OnlyIfException {
            boolean result;
            if (methodName.endsWith("()")) {
                methodName = methodName.substring(0, methodName.length() - 2); // snip off the "()"
                result = execOnlyIfMethod(methodName);
            }
            else {
                result = getOnlyIfField(methodName);
            }
            return result == mustEqual;
        }


        private boolean execOnlyIfMethod(String methodName) throws OnlyIfException {
            final Object result;
            final Method onlyIfMethod;
            final Class<?> testClass = getTestClass().getJavaClass();
            try {
                onlyIfMethod = testClass.getMethod(methodName);
            } catch (NoSuchMethodException e) {
                throw new OnlyIfException("no such method: public boolean " + methodName + "()", e);
            }
            if (!onlyIfMethod.getReturnType().equals(boolean.class)) {
                throw new OnlyIfException("no such method: public boolean " + methodName + "()");
            }
            try {
                Object test = createTest();
                result = onlyIfMethod.invoke(test);
            } catch (Exception e) {
                throw new OnlyIfException("couldn't invoke " + methodName + "()", e);
            }
            try {
                return (Boolean) result;
            } catch (ClassCastException e) {
                throw new OnlyIfException("method " + methodName + "() didn't return boolean", e);
            }
        }

        private boolean getOnlyIfField(String fieldName) throws OnlyIfException {
            final Class<?> testClass = getTestClass().getJavaClass();
            final Field field;
            try {
                field = testClass.getField(fieldName);
            } catch (Exception e) {
                throw new OnlyIfException("Can't get field: " + fieldName, e);
            }
            try {
                Object test = createTest();
                return field.getBoolean(test);
            } catch (Exception e) {
                throw new OnlyIfException("couldn't get boolean field " + fieldName + "()", e);
            }
        }

        boolean expectedToPass(FrameworkMethod method)
        {
            if (overrideOn)
            {
                return true;
            }
            if (! parameterization.expectedToPass())
            {
                return false;
            }
            Failing failing = method.getAnnotation(Failing.class);
            if (failing == null)
            {
                return true;
            }
            if (failing.value().length == 0)
            {
                return false;
            }

            for (final String paramName : failing.value())
            {
                if (paramNameUsesRegex(paramName))
                {
                    if (paramNameMatchesRegex(parameterization.getName(), paramName))
                    {
                        return false;
                    }
                }
                else if (parameterization.getName().equals(paramName))
                {
                    return false;
                }
            }
            return true;
        }
    }

    static boolean paramNameUsesRegex(String paramName)
    {
        return paramName.length() > 2
                && (paramName.charAt(0)=='/')
                && (paramName.charAt(paramName.length()-1)=='/');
    }

    /**
     * Returns whether a given parameterization matches a given regex. The regex should be in "/regex/" format.
     * @param paramName the haystack, as it were
     * @param paramRegex a string that starts and ends with '/', and between them has a needle.
     * @return whether the paramRegex is found in paramName
     */
    static boolean paramNameMatchesRegex(String paramName, String paramRegex)
    {
        assert paramRegex.charAt(0)=='/';
        assert paramRegex.charAt(paramRegex.length()-1)=='/';
        assert paramRegex.length() > 2;
        String regex = paramRegex.substring(1, paramRegex.length()-1);
        return Pattern.compile(regex).matcher(paramName).find();
    }

    @SuppressWarnings("unused") // Invoked by reflection
	public NamedParameterizedRunner(Class<?> klass) throws Throwable
    {
        super(klass, Collections.<Runner>emptyList());

        if (getTestClass().getJavaClass().getAnnotation(Ignore.class) != null)
        {
            runners = Collections.unmodifiableList(Arrays.asList((Runner)new IgnoredClassRunner(klass)));
            return;
        }

        List<Runner> localRunners = new LinkedList<>();

        Collection<Parameterization> parameterizations = getParameterizations();
        checkFailingParameterizations(parameterizations);

        final String override = System.getProperty(PARAMETERIZATION_OVERRIDE);
        final boolean overrideIsRegex = (override != null) && paramNameUsesRegex(override);
        if (override != null)
        {
            String msg = "Override is set to";
            if (overrideIsRegex)
            {
                msg += " regex";
            }
            msg += ":" + override;
            logger.debug(msg);
        }
        for (Parameterization param : parameterizations)
        {
            final boolean useThisParam;
            if (override == null)
            {
                useThisParam = true;
            }
            else if (overrideIsRegex)
            {
                useThisParam = paramNameMatchesRegex(param.getName(), override);
            }
            else
            {
                useThisParam = param.getName().equals(override);
            }
            if (useThisParam)
            {
                if (override != null)
                {
                    logger.debug("Adding parameterization: " + param.getName());
                }
                localRunners.add(new ReifiedParamRunner(getTestClass().getJavaClass(), param, override != null));
            }
        }
        runners = Collections.unmodifiableList(localRunners);
    }

	@Override
	protected List<Runner> getChildren()
    {
		return runners;
	}

    /**
     * Gets the parameterization
     * @return the parameterization collection
     * @throws Throwable if the annotation requirements are not met, or if there's an error in invoking
     * the class's "get parameterizations" method.
     */
    private Collection<Parameterization> getParameterizations() throws Throwable
    {
        TestClass cls = getTestClass();
        List<FrameworkMethod> methods = cls.getAnnotatedMethods(TestParameters.class);

        if (methods.size() != 1)
        {
            throw new Exception("class " + cls.getName() + " must have exactly 1 method annotated with "
                + TestParameters.class.getSimpleName() +"; found " + methods.size());
        }

        FrameworkMethod method = methods.get(0);
        checkParameterizationMethod(method);

        @SuppressWarnings("unchecked")
        Collection<Parameterization> ret = (Collection<Parameterization>) method.invokeExplosively(null);
        checkParameterizations(ret);
        return ret;
    }

    /**
     * Checks the parameterizations collection for correctness
     * @param collection the collection
     * @throws Exception if the collection was null or contained duplicates
     */
    private void checkParameterizations(Collection<Parameterization> collection) throws Exception
    {
        if (collection == null)
        {
            throw new Exception("parameterizations collection may not return null");
        }
        Set<String> duplicates = new HashSet<>();
        Set<Parameterization> checkSet = new HashSet<>();
        for (Parameterization param : collection)
        {
            if (param == null)
            {
                throw new Exception("parameterization collection may not contain null values");
            }
            if (!checkSet.add(param))
            {
                duplicates.add(param.getName());
            }
        }
        if (duplicates.size() != 0)
        {
            throw new Exception("duplicated parameterization names: " + duplicates);
        }
    }

    /**
     * Checks the @Failing annotations on @Test methods.
     *
     * Specifically, this checks that no item in the @Failing list is repeated, and that each item in that list
     * corresponds to an actual parameterization.
     * @param parameterizations the known parameterizations; will not be modified.
     * @throws org.junit.runners.model.InitializationError if a check fails
     */
    private void checkFailingParameterizations(Collection<Parameterization> parameterizations) throws InitializationError
    {
        Collection<String> paramNames = new HashSet<>();
        for (Parameterization param : parameterizations)
        {
            boolean added = paramNames.add(param.getName());
            assert added : "uncaught duplicate: " + param;
        }

        Set<String> duplicatesChecker = new HashSet<>();
        for(FrameworkMethod method : super.getTestClass().getAnnotatedMethods(Failing.class))
        {
            String[] failing = method.getAnnotation(Failing.class).value();
            if (failing != null)
            {
                for(int i=0; i < failing.length; ++i)
                {
                    if (!duplicatesChecker.add(failing[i]))
                    {
                        throw new InitializationError("duplicate parameterization name in @Failing list: "
                                + method.getName() + "[" + i + "]<" + failing[i] + ">");
                    }
                    if ( !(paramNameUsesRegex(failing[i]) || paramNames.contains(failing[i])) )
                    {
                        throw new InitializationError("parameterization is marked as failing, "
                                + "but isn't in list of parameterizations: "
                                + method.getName() + "[" + i + "]<" + failing[i] +"> not in " + paramNames);
                    }
                }
                duplicatesChecker.clear();
            }
        }
    }


    /**
     * Checks the parameterization method for correctness.
     * @param frameworkMethod the method
     * @throws Exception if the annotation requirements are not met
     */
    private static void checkParameterizationMethod(FrameworkMethod frameworkMethod) throws Exception
    {
        final Method method = frameworkMethod.getMethod();

        if (method.getParameterTypes().length != 0)
        {
            throw new Exception(complainingThat(method, "must take no arguments"));
        }

        final int modifiers = frameworkMethod.getMethod().getModifiers();
        if (! Modifier.isPublic(modifiers))
        {
            throw new Exception(complainingThat(method, "must be public"));
        }
        if (! Modifier.isStatic(modifiers))
        {
            throw new Exception(complainingThat(method, "must be static"));
        }

        final Type genericRet = method.getGenericReturnType();
        final String mustReturnCorrectly = "must return Collection of " + Parameterization.class;
        if (! (genericRet instanceof ParameterizedType))
        {
            throw new Exception(complainingThat(method, mustReturnCorrectly));
        }
        final ParameterizedType ret = (ParameterizedType) genericRet;
        if (!(ret.getRawType() instanceof Class) && Collection.class.isAssignableFrom((Class)ret.getRawType()))
        {
            throw new Exception(complainingThat(method, mustReturnCorrectly));
        }
        if (ret.getActualTypeArguments().length != 1)
        {
            throw new Exception(complainingThat(method, mustReturnCorrectly + "; raw Collection is not allowed"));
        }
        if (!ret.getActualTypeArguments()[0].equals(Parameterization.class))
        {
            throw new Exception(complainingThat(method, mustReturnCorrectly));
        }
    }

    private static String complainingThat(Method method, String mustBe)
    {
        assert method != null;
        assert mustBe != null;
        return String.format("%s.%s() %s", method.getDeclaringClass().getName(), method.getName(), mustBe);
    }
}
