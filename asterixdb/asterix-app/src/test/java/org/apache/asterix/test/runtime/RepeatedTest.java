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
package org.apache.asterix.test.runtime;

import java.util.Collection;

import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.hyracks.test.support.RepeatRule;
import org.apache.hyracks.test.support.RepeatRule.Repeat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs runtime test cases that have been identified in the repeatedtestsuite.xml.
 * Each test is run 10000 times.
 */

@RunWith(Parameterized.class)
public class RepeatedTest extends SqlppExecutionTest {

    private int count;

    @Parameters(name = "RepeatedTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.buildTestsInXml(TestCaseContext.DEFAULT_REPEATED_TESTSUITE_XML_NAME);
    }

    public RepeatedTest(TestCaseContext tcCtx) {
        super(tcCtx);
        count = 0;
    }

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @Override
    @Test
    @Repeat(times = 100)
    public void test() throws Exception {
        System.err.println("***** Test Count: " + (++count) + " ******");
        super.test();
    }
}
