/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metrics.scalar;

import static org.apache.ignite.internal.util.IgniteUtils.onNodeStart;
import static org.apache.ignite.internal.util.IgniteUtils.onNodeStop;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Hit rate metric test.
 */
public class HitRateMetricTest {
    @BeforeEach
    public void beforeAll() {
        onNodeStart();
    }

    @AfterEach
    public void afterAll() {
        onNodeStop();
    }

    @Test
    public void testHitrateMetric() {
        HitRateMetric hitRateMetric = new HitRateMetric("hitRate", null, 100);

        hitRateMetric.increment();

        doSleep(5);
        hitRateMetric.add(2);

        doSleep(20);

        hitRateMetric.increment();

        assertEquals(4, hitRateMetric.value());

        doSleep(100);
        hitRateMetric.increment();

        assertEquals(1, hitRateMetric.value());
    }

    private void doSleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // No-op.
        }
    }
}
