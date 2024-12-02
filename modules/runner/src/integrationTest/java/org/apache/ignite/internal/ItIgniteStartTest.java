/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.parser.ConfigDocument;
import com.typesafe.config.parser.ConfigDocumentFactory;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.Cluster.ServerRegistration;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Test;

class ItIgniteStartTest extends ClusterPerTestIntegrationTest {
    private static final long RAFT_RETRY_TIMEOUT_MILLIS = 2500;

    @Override
    protected int initialNodes() {
        return 0;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        ConfigDocument document = ConfigDocumentFactory.parseString(super.getNodeBootstrapConfigTemplate())
                .withValueText("ignite.raft.retryTimeout", Long.toString(RAFT_RETRY_TIMEOUT_MILLIS));
        return document.render();
    }

    @Test
    void nodeStartDoesntTimeoutWhenCmgIsUnavailable() throws Exception {
        int nodeCount = 2;
        cluster.startAndInit(nodeCount, new int[]{0, 1});

        IntStream.range(0, nodeCount).parallel().forEach(nodeIndex -> cluster.stopNode(nodeIndex));

        ServerRegistration registration0 = cluster.startEmbeddedNode(0);

        // Now allow an attempt to join to timeout (if it doesn't have an infinite timeout).
        waitTillRaftTimeoutPasses();

        assertDoesNotThrow(() -> cluster.startNode(1));

        assertThat(registration0.registrationFuture(), willCompleteSuccessfully());
    }

    private static void waitTillRaftTimeoutPasses() throws InterruptedException {
        Thread.sleep(RAFT_RETRY_TIMEOUT_MILLIS + 1000);
    }

    @Test
    void nodeStartDoesntTimeoutWhenMgIsUnavailable() throws Exception {
        int nodeCount = 3;
        cluster.startAndInit(nodeCount, builder -> {
            builder.cmgNodeNames(cluster.nodeName(0), cluster.nodeName(1));
            builder.metaStorageNodeNames(cluster.nodeName(1), cluster.nodeName(2));
        });

        IntStream.range(0, nodeCount).parallel().forEach(nodeIndex -> cluster.stopNode(nodeIndex));

        // These 2 nodes have majority of CMG, but not MG.
        ServerRegistration registration0 = cluster.startEmbeddedNode(0);
        ServerRegistration registration1 = cluster.startEmbeddedNode(1);

        waitTill2NodesValidateThemselvesWithCmg(registration0);

        // Now allow an attempt to recover Metastorage to timeout (if it doesn't have an infinite timeout).
        waitTillRaftTimeoutPasses();

        assertDoesNotThrow(() -> cluster.startNode(2));

        assertThat(registration0.registrationFuture(), willCompleteSuccessfully());
        assertThat(registration1.registrationFuture(), willCompleteSuccessfully());
    }

    private static void waitTill2NodesValidateThemselvesWithCmg(ServerRegistration registration) throws InterruptedException {
        IgniteImpl ignite = ((IgniteServerImpl) registration.server()).igniteImpl();

        assertTrue(
                waitForCondition(() -> validatedNodes(ignite).size() == 2, SECONDS.toMillis(10)),
                "Did not see 2 nodes being validated in time after restart"
        );
    }

    private static Set<ClusterNode> validatedNodes(IgniteImpl ignite) {
        CompletableFuture<Set<ClusterNode>> validatededNodesFuture = ignite.clusterManagementGroupManager().validatedNodes();
        assertThat(validatededNodesFuture, willCompleteSuccessfully());
        return validatededNodesFuture.join();
    }
}
