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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.framework.ImplicitTxContext;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.table.distributed.replicator.InternalSchemaVersionMismatchException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryRetryTest extends BaseIgniteAbstractTest {
    private TestCluster cluster;

    @BeforeEach
    public void init() {
        cluster = TestBuilders.cluster()
                .nodes("N1")
                .dataProvider("N1", "TEST_TBL", new ScannableTable() {
                    @Override
                    public <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                            RowFactory<RowT> rowFactory, @Nullable BitSet requiredColumns) {
                        return subscriber -> subscriber.onError(new InternalSchemaVersionMismatchException());
                    }

                    @Override
                    public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx,
                            PartitionWithConsistencyToken partWithConsistencyToken, RowFactory<RowT> rowFactory, int indexId,
                            List<String> columns, @Nullable RangeCondition<RowT> cond, @Nullable BitSet requiredColumns) {
                        return null;
                    }

                    @Override
                    public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx,
                            PartitionWithConsistencyToken partWithConsistencyToken, RowFactory<RowT> rowFactory, int indexId,
                            List<String> columns, RowT key, @Nullable BitSet requiredColumns) {
                        return null;
                    }

                    @Override
                    public <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(ExecutionContext<RowT> ctx,
                            @Nullable InternalTransaction explicitTx, RowFactory<RowT> rowFactory, RowT key,
                            @Nullable BitSet requiredColumns) {
                        return CompletableFuture.failedFuture(new InternalSchemaVersionMismatchException());
                    }
                })
                .build();

        cluster.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        cluster.stop();
    }

    @Test
    void testQuery() {
        TestNode node = cluster.node("N1");

        node.initSchema("CREATE TABLE test_tbl (ID INT PRIMARY KEY, VAL INT, VAL2 INT)");

        QueryPlan plan = node.prepare("SELECT * FROM test_tbl");

        node.initSchema("ALTER TABLE test_tbl DROP COLUMN VAL2");
        ImplicitTxContext.INSTANCE.updateObservableTime(new HybridClockImpl().now());

        assertThrows(InternalSchemaVersionMismatchException.class, () -> node.executePlan(plan), null);
    }

    @Test
    void tesPkLookup() {
        TestNode node = cluster.node("N1");

        node.initSchema("CREATE TABLE test_tbl (ID INT PRIMARY KEY, VAL INT, VAL2 INT)");

        QueryPlan plan = node.prepare("SELECT * FROM test_tbl WHERE id = 1");

        node.initSchema("ALTER TABLE test_tbl DROP COLUMN VAL2");
        ImplicitTxContext.INSTANCE.updateObservableTime(new HybridClockImpl().now());

        assertThrows(InternalSchemaVersionMismatchException.class, () -> node.executePlan(plan).requestNextAsync(10).get(), null);
    }

    @Test
    void testDmlQuery() {
        TestNode node = cluster.node("N1");

        node.initSchema("CREATE TABLE test_tbl (ID INT PRIMARY KEY, VAL INT, VAL2 INT)");

        QueryPlan plan = node.prepare("INSERT INTO test_tbl VALUES (1, 2, 3)");

        node.initSchema("ALTER TABLE test_tbl DROP COLUMN VAL2");
        ImplicitTxContext.INSTANCE.updateObservableTime(new HybridClockImpl().now());

        assertThrows(InternalSchemaVersionMismatchException.class, () -> node.executePlan(plan).requestNextAsync(10).get(), null);
    }
}