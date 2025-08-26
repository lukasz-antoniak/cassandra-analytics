/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.testing;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.apache.commons.lang3.StringUtils;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.extension.DynamicTestInvocationContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import com.vdurmont.semver4j.Semver;

public class CassandraVersionRequirementExtension implements InvocationInterceptor
{
    public <T> T interceptTestClassConstructor(Invocation<T> invocation,
                                               ReflectiveInvocationContext<Constructor<T>> invocationContext,
                                               ExtensionContext extensionContext) throws Throwable
    {
        CassandraVersionRequirement versionRequirement = extensionContext
                                                         .getRequiredTestClass()
                                                         .getAnnotation(CassandraVersionRequirement.class);
        skipIfVersionOutOfScope(versionRequirement);
        return invocation.proceed();
    }

    public void interceptTestMethod(Invocation<Void> invocation,
                                    ReflectiveInvocationContext<Method> invocationContext,
                                    ExtensionContext extensionContext) throws Throwable
    {
        interceptTestMethod(invocation, extensionContext);
    }

    public void interceptDynamicTest(Invocation<Void> invocation,
                                     DynamicTestInvocationContext invocationContext,
                                     ExtensionContext extensionContext) throws Throwable
    {
        interceptTestMethod(invocation, extensionContext);
    }

    public void interceptTestTemplateMethod(Invocation<Void> invocation,
                                            ReflectiveInvocationContext<Method> invocationContext,
                                            ExtensionContext extensionContext) throws Throwable
    {
        interceptTestMethod(invocation, extensionContext);
    }

    private void interceptTestMethod(Invocation<Void> invocation,
                                     ExtensionContext extensionContext) throws Throwable
    {
        CassandraVersionRequirement versionRequirement = extensionContext
                                                         .getRequiredTestMethod()
                                                         .getAnnotation(CassandraVersionRequirement.class);
        skipIfVersionOutOfScope(versionRequirement);
        invocation.proceed();
    }

    private void skipIfVersionOutOfScope(CassandraVersionRequirement versionRequirement)
    {
        if (versionRequirement != null)
        {
            Semver clusterVersion = TestUtils.getDTestClusterVersion();
            if (!acceptClusterVersion(versionRequirement, clusterVersion))
            {
                throw new AssumptionViolatedException(versionRequirement.description());
            }
        }
    }

    private boolean acceptClusterVersion(CassandraVersionRequirement required, Semver actual)
    {
        if (!StringUtils.isEmpty(required.minimum()))
        {
            Semver ver = new Semver(required.minimum(), Semver.SemverType.LOOSE);
            if (actual.isLowerThan(ver))
            {
                return false;
            }
        }
        if (!StringUtils.isEmpty(required.maximum()))
        {
            Semver ver = new Semver(required.maximum(), Semver.SemverType.LOOSE);
            if (actual.isGreaterThan(ver))
            {
                return false;
            }
        }
        return true;
    }
}
