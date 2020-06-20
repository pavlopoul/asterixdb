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
package org.apache.asterix.test.statistics;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.bootstrap.TestNodeController.SecondaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.data.gen.RecordTupleGenerator.GenerationFunction;
// import org.apache.asterix.app.data.gen.TupleGenerator;
// import org.apache.asterix.app.data.gen.TupleGenerator.GenerationFunction;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.runtime.operators.LSMPrimaryUpsertOperatorNodePushable;
import org.apache.asterix.statistics.TestMetadataProvider;
import org.apache.asterix.statistics.TestMetadataProvider.TestStatisticsEntry;
import org.apache.asterix.statistics.TestMetadataProvider.TestStatisticsID;
import org.apache.asterix.statistics.message.TestStatisticsMessageBroker;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.test.dataflow.StorageTestUtils;
import org.apache.asterix.test.dataflow.TestCountingStatisticsFactory.CountingSynopsis;
import org.apache.asterix.test.dataflow.TestDataset;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.nc.application.NCServiceContext;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.test.support.TestStorageManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class StatisticsTest {
    private static TestNodeController nc;
    private static TestStatisticsMessageBroker msgBroker;
    private static TestMetadataProvider testMdProvider;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static IHyracksTaskContext ctx;
    private static ITransactionContext txnCtx;
    private static TestLsmBtree primaryLsmBtree;
    private static TestLsmBtree secondaryLsmBtree;
    private static IIndexDataflowHelper[] indexDataflowHelpers = new IIndexDataflowHelper[2];
    private static IStorageManager storageManager;
    private static final int PARTITION = 0;
    private static LSMPrimaryInsertOperatorNodePushable insertOp;
    private static LSMInsertDeleteOperatorNodePushable deleteOp;
    private static LSMPrimaryUpsertOperatorNodePushable upsertOp;
    private static int NUM_INSERT_RECORDS = 1000;
    private static RecordTupleGenerator insertOpTupleGenerator;
    private static RecordTupleGenerator deleteOpTupleGenerator;
    private static final String KEY_FIELD_NAME = "key";
    private static final String INDEXED_FIELD_NAME = "indexed_value";
    private static final IAType KEY_FIELD_TYPE = BuiltinType.AINT32;
    private static final IAType INDEXED_FIELD_TYPE = BuiltinType.AINT64;
    private static final String[] FIELD_NAMES = new String[] { KEY_FIELD_NAME, INDEXED_FIELD_NAME };
    private static final IAType[] FIELD_TYPES = new IAType[] { KEY_FIELD_TYPE, INDEXED_FIELD_TYPE };
    private static final ARecordType RECORD_TYPE = new ARecordType("TestRecordType", FIELD_NAMES, FIELD_TYPES, false);

    private static final String INDEX_NAME = "TestIdx";
    private static final List<List<String>> INDEX_FIELD_NAMES = Arrays.asList(Arrays.asList(INDEXED_FIELD_NAME));
    private static final List<IAType> INDEX_FIELD_TYPES = Arrays.asList(INDEXED_FIELD_TYPE);
    private static final List<Integer> INDEX_FIELD_INDICATORS = Arrays.asList(Index.RECORD_INDICATOR);

    private static final TestDataset STATS_DATASET = new TestDataset(StorageTestUtils.DATAVERSE_NAME,
            StorageTestUtils.DATASET_NAME, StorageTestUtils.DATAVERSE_NAME, StorageTestUtils.DATA_TYPE_NAME,
            StorageTestUtils.NODE_GROUP_NAME, NoMergePolicyFactory.NAME, null,
            new InternalDatasetDetails(null, PartitioningStrategy.HASH, StorageTestUtils.PARTITIONING_KEYS, null, null,
                    null, false, null),
            new HashMap<>(), DatasetType.INTERNAL, StorageTestUtils.DATASET_ID, 0, true, true);
    private static final TestStatisticsID keyFieldStatisticsID =
            new TestStatisticsID(StorageTestUtils.DATAVERSE_NAME.getCanonicalForm(), StorageTestUtils.DATASET_NAME,
                    StorageTestUtils.DATASET_NAME, KEY_FIELD_NAME, "asterix_nc1", "partition_0", false);
    private static final TestStatisticsID keyFieldAntimatterStatisticsID =
            new TestStatisticsID(StorageTestUtils.DATAVERSE_NAME.getCanonicalForm(), StorageTestUtils.DATASET_NAME,
                    StorageTestUtils.DATASET_NAME, KEY_FIELD_NAME, "asterix_nc1", "partition_0", true);
    private static final TestStatisticsID indexedFieldStatisticsID =
            new TestStatisticsID(StorageTestUtils.DATAVERSE_NAME.getCanonicalForm(), StorageTestUtils.DATASET_NAME,
                    INDEX_NAME, INDEXED_FIELD_NAME, "asterix_nc1", "partition_0", false);
    private static final TestStatisticsID indexedFieldAntimatterStatisticsID =
            new TestStatisticsID(StorageTestUtils.DATAVERSE_NAME.getCanonicalForm(), StorageTestUtils.DATASET_NAME,
                    INDEX_NAME, INDEXED_FIELD_NAME, "asterix_nc1", "partition_0", true);

    @BeforeClass
    public static void startTestCluster() throws Exception {
        TestHelper.deleteExistingInstanceFiles();
        String configPath = Paths.get(System.getProperty("user.dir"), "src", "test", "resources", "cc.conf").toString();
        nc = new TestNodeController(configPath, false);
        nc.init();
        NCAppRuntimeContext ncAppCtx = nc.getAppRuntimeContext();
        NCServiceContext ncCtx = (NCServiceContext) ncAppCtx.getServiceContext();
        testMdProvider = new TestMetadataProvider();
        msgBroker = new TestStatisticsMessageBroker(testMdProvider);
        ncCtx.setMessageBroker(msgBroker);
        dsLifecycleMgr = ncAppCtx.getDatasetLifecycleManager();
        storageManager = new TestStorageManager() {
            @Override
            public IResourceLifecycleManager<IIndex> getLifecycleManager(INCServiceContext ctx) {
                return dsLifecycleMgr;
            }

            @Override
            public ILocalResourceRepository getLocalResourceRepository(INCServiceContext ctx) {
                return ncAppCtx.getLocalResourceRepository();
            }
        };
    }

    @AfterClass
    public static void stopTestCluster() throws Exception {
        nc.deInit();
        TestHelper.deleteExistingInstanceFiles();
    }

    @Before
    public void setUp() throws Exception {
        insertOpTupleGenerator = new RecordTupleGenerator(RECORD_TYPE, StorageTestUtils.META_TYPE,
                StorageTestUtils.KEY_INDEXES, StorageTestUtils.KEY_INDICATORS,
                new GenerationFunction[] { GenerationFunction.INCREASING, GenerationFunction.INCREASING },
                StorageTestUtils.UNIQUE_RECORD_FIELDS, StorageTestUtils.META_GEN_FUNCTION,
                StorageTestUtils.UNIQUE_META_FIELDS);
        //StorageTestUtils.getTupleGenerator();
        deleteOpTupleGenerator = new RecordTupleGenerator(RECORD_TYPE, StorageTestUtils.META_TYPE,
                StorageTestUtils.KEY_INDEXES, StorageTestUtils.KEY_INDICATORS,
                new GenerationFunction[] { GenerationFunction.INCREASING, GenerationFunction.INCREASING },
                StorageTestUtils.UNIQUE_RECORD_FIELDS, StorageTestUtils.META_GEN_FUNCTION,
                StorageTestUtils.UNIQUE_META_FIELDS);
        createIndex();
        testMdProvider.clearStats();
    }

    private void createIndex() throws Exception {
        StorageComponentProvider storageProvider = new StorageComponentProvider() {
            @Override
            public IStorageManager getStorageManager() {
                return storageManager;
            }
        };
        PrimaryIndexInfo primaryIndexInfo = nc.createPrimaryIndex(STATS_DATASET, StorageTestUtils.KEY_TYPES,
                RECORD_TYPE, StorageTestUtils.META_TYPE, null, storageProvider, StorageTestUtils.KEY_INDEXES,
                StorageTestUtils.KEY_INDICATORS_LIST, PARTITION);
        Index secondaryIndex = new Index(StorageTestUtils.DATAVERSE_NAME, StorageTestUtils.DATASET_NAME, INDEX_NAME,
                IndexType.BTREE, INDEX_FIELD_NAMES, INDEX_FIELD_INDICATORS, INDEX_FIELD_TYPES, false, false, false, 0);
        SecondaryIndexInfo secondaryIndexInfo =
                nc.createSecondaryIndex(primaryIndexInfo, secondaryIndex, storageProvider, 0);
        IndexDataflowHelperFactory[] idxHelperFactories = new IndexDataflowHelperFactory[] {
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider()),
                new IndexDataflowHelperFactory(nc.getStorageManager(), secondaryIndexInfo.getFileSplitProvider()) };
        JobId jobId = nc.newJobId();
        ctx = nc.createTestContext(jobId, PARTITION, false);
        txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(jobId),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
        for (int i = 0; i < idxHelperFactories.length; i++) {
            indexDataflowHelpers[i] =
                    idxHelperFactories[i].create(ctx.getJobletContext().getServiceContext(), PARTITION);
            indexDataflowHelpers[i].open();
            TestLsmBtree tree = (TestLsmBtree) indexDataflowHelpers[i].getIndexInstance();
            if (i == 0) {
                primaryLsmBtree = tree;
            } else {
                secondaryLsmBtree = tree;
            }
            indexDataflowHelpers[i].close();
            StorageTestUtils.allowAllOps(tree);
        }

        insertOp = nc.getInsertPipeline(ctx, STATS_DATASET, StorageTestUtils.KEY_TYPES, RECORD_TYPE,
                StorageTestUtils.META_TYPE, null, StorageTestUtils.KEY_INDEXES, StorageTestUtils.KEY_INDICATORS_LIST,
                storageProvider, secondaryIndex, null).getLeft();
        deleteOp = nc.getDeletePipeline(ctx, STATS_DATASET, StorageTestUtils.KEY_TYPES, RECORD_TYPE,
                StorageTestUtils.META_TYPE, null, StorageTestUtils.KEY_INDEXES, StorageTestUtils.KEY_INDICATORS_LIST,
                storageProvider, secondaryIndex).getLeft();
        upsertOp = nc.getUpsertPipeline(ctx, STATS_DATASET, StorageTestUtils.KEY_TYPES, RECORD_TYPE,
                StorageTestUtils.META_TYPE, null, StorageTestUtils.KEY_INDEXES, StorageTestUtils.KEY_INDICATORS_LIST,
                storageProvider, null, true).getLeft();
    }

    //TODO: add tests for non-indexed fields

    @After
    public void tearDown() throws Exception {
        for (int i = 0; i < indexDataflowHelpers.length; i++) {
            indexDataflowHelpers[i].destroy();
        }
    }

    private void insertRecords(int numRecords) throws Exception {
        ingestRecords(numRecords, insertOp, insertOpTupleGenerator);
    }

    private void deleteRecords(int numRecords) throws Exception {
        ingestRecords(numRecords, deleteOp, deleteOpTupleGenerator);
    }

    private void upsertRecords(int numRecords) throws Exception {
        ingestRecords(numRecords, upsertOp, deleteOpTupleGenerator);
    }

    private void ingestRecords(int numRecords, IndexInsertUpdateDeleteOperatorNodePushable opNodePushable,
            RecordTupleGenerator tupleGenerator) throws Exception {
        opNodePushable.open();
        VSizeFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
        ITupleReference tuple;
        for (int i = 0; i < numRecords; i++) {
            tuple = tupleGenerator.next();
            DataflowUtils.addTupleToFrame(tupleAppender, tuple, opNodePushable);
        }
        if (tupleAppender.getTupleCount() > 0) {
            tupleAppender.write(opNodePushable, true);
        }
        opNodePushable.close();
    }

    @Test
    public void testFlushEmptyStatistics() throws Exception {
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        // flush statistics without inserting any record
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsEntry> keyFieldStats = testMdProvider.getStats(keyFieldStatisticsID);
        Collection<TestStatisticsEntry> valueFieldStats = testMdProvider.getStats(indexedFieldStatisticsID);
        Collection<TestStatisticsEntry> keyFieldAntimatter = testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsEntry> valueFieldAntimatterStats =
                testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        Assert.assertTrue(keyFieldStats.isEmpty());
        Assert.assertTrue(valueFieldStats.isEmpty());
        Assert.assertTrue(keyFieldAntimatter.isEmpty());
        Assert.assertTrue(valueFieldAntimatterStats.isEmpty());
    }

    @Test
    public void testFlushInsertDeleteEmptyStatistics() throws Exception {
        // insert 1000 records...
        insertRecords(NUM_INSERT_RECORDS);
        // ...and delete them right after
        deleteRecords(NUM_INSERT_RECORDS);
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        // flush statistics after deletion all of the records
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsEntry> keyFieldStats = testMdProvider.getStats(keyFieldStatisticsID);
        Collection<TestStatisticsEntry> valueFieldStats = testMdProvider.getStats(indexedFieldStatisticsID);
        Collection<TestStatisticsEntry> keyFieldAntimatterStats =
                testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsEntry> valueFieldAntimatterStats =
                testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        Assert.assertTrue(keyFieldStats.isEmpty());
        Assert.assertTrue(valueFieldStats.isEmpty());
        Assert.assertTrue(keyFieldAntimatterStats.isEmpty());
        Assert.assertTrue(valueFieldAntimatterStats.isEmpty());
    }

    @Test
    public void testFlushStatistics() throws Exception {
        insertRecords(NUM_INSERT_RECORDS);
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsEntry> keyFieldStatsEntries = testMdProvider.getStats(keyFieldStatisticsID);
        Collection<TestStatisticsEntry> valueFieldStatsEntries = testMdProvider.getStats(indexedFieldStatisticsID);
        Collection<TestStatisticsEntry> keyFieldAntimatterStatsEntries =
                testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsEntry> valueFieldAntimatterStatsEntries =
                testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        Assert.assertNotNull(keyFieldStatsEntries);
        Assert.assertEquals(1, keyFieldStatsEntries.size());
        TestStatisticsEntry keyFieldEntry = keyFieldStatsEntries.iterator().next();
        CountingSynopsis synopsis = (CountingSynopsis) keyFieldEntry.getSynopsis();
        Assert.assertEquals(keyFieldEntry.getComponentId().getMinTimestamp(),
                keyFieldEntry.getComponentId().getMaxTimestamp());
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());

        Assert.assertNotNull(valueFieldStatsEntries);
        Assert.assertEquals(1, valueFieldStatsEntries.size());
        TestStatisticsEntry valueFieldEntry = valueFieldStatsEntries.iterator().next();
        synopsis = (CountingSynopsis) valueFieldEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        Assert.assertEquals(valueFieldEntry.getComponentId().getMinTimestamp(),
                valueFieldEntry.getComponentId().getMaxTimestamp());

        Assert.assertTrue(keyFieldAntimatterStatsEntries.isEmpty());
        Assert.assertTrue(valueFieldAntimatterStatsEntries.isEmpty());
    }

    @Test
    public void testFlushAntimatterStatistics() throws Exception {
        // insert 500 records...
        insertRecords(NUM_INSERT_RECORDS / 2);
        // and delete 1000 records right after, generating 500 antimatter entries
        deleteRecords(NUM_INSERT_RECORDS);
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());

        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsEntry> keyFieldStatsEntries = testMdProvider.getStats(keyFieldStatisticsID);
        Collection<TestStatisticsEntry> valueFieldStatsEntries = testMdProvider.getStats(indexedFieldStatisticsID);
        Collection<TestStatisticsEntry> keyFieldAntimatterStatsEntries =
                testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsEntry> valueFieldAntimatterStatsEntries =
                testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        Assert.assertTrue(keyFieldStatsEntries.isEmpty());
        Assert.assertTrue(valueFieldStatsEntries.isEmpty());
        Assert.assertNotNull(keyFieldAntimatterStatsEntries);
        Assert.assertEquals(1, keyFieldAntimatterStatsEntries.size());
        TestStatisticsEntry antimatterKeyEntry = keyFieldAntimatterStatsEntries.iterator().next();
        Assert.assertEquals(antimatterKeyEntry.getComponentId().getMinTimestamp(),
                antimatterKeyEntry.getComponentId().getMaxTimestamp());
        CountingSynopsis synopsis = (CountingSynopsis) antimatterKeyEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());

        Assert.assertNotNull(valueFieldAntimatterStatsEntries);
        Assert.assertEquals(1, valueFieldAntimatterStatsEntries.size());
        TestStatisticsEntry antimatterValueEntry = valueFieldAntimatterStatsEntries.iterator().next();
        Assert.assertEquals(antimatterValueEntry.getComponentId().getMinTimestamp(),
                antimatterValueEntry.getComponentId().getMaxTimestamp());
        synopsis = (CountingSynopsis) antimatterValueEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());
    }

    @Test
    public void testMergeStatisticsReconcileAll() throws Exception {
        // insert 1000 records...
        insertRecords(NUM_INSERT_RECORDS);
        // flush and generate statistics about 1000 records...
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsEntry> keyFieldStats = testMdProvider.getStats(keyFieldStatisticsID);
        Collection<TestStatisticsEntry> valueFieldStats = testMdProvider.getStats(indexedFieldStatisticsID);
        Collection<TestStatisticsEntry> keyFieldAntimatterStats =
                testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsEntry> valueFieldAntimatterStats =
                testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        Assert.assertNotNull(keyFieldStats);
        Assert.assertEquals(1, keyFieldStats.size());
        TestStatisticsEntry keyFieldStatsEntry = keyFieldStats.iterator().next();
        Assert.assertEquals(keyFieldStatsEntry.getComponentId().getMinTimestamp(),
                keyFieldStatsEntry.getComponentId().getMaxTimestamp());
        CountingSynopsis synopsis = (CountingSynopsis) keyFieldStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());

        Assert.assertNotNull(valueFieldStats);
        Assert.assertEquals(1, valueFieldStats.size());
        TestStatisticsEntry valueFieldStatsEntry = valueFieldStats.iterator().next();
        Assert.assertEquals(valueFieldStatsEntry.getComponentId().getMinTimestamp(),
                valueFieldStatsEntry.getComponentId().getMaxTimestamp());
        synopsis = (CountingSynopsis) valueFieldStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());

        Assert.assertTrue(keyFieldAntimatterStats.isEmpty());
        Assert.assertTrue(valueFieldAntimatterStats.isEmpty());

        // ... and delete all 1000 records
        deleteRecords(NUM_INSERT_RECORDS);
        //accessors.scheduleFlush(lsmBtree.getIOOperationCallback());
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        keyFieldStats = testMdProvider.getStats(keyFieldStatisticsID);
        valueFieldStats = testMdProvider.getStats(indexedFieldStatisticsID);
        keyFieldAntimatterStats = testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        valueFieldAntimatterStats = testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        // reguar stats were not changed
        Assert.assertEquals(1, keyFieldStats.size());
        Assert.assertEquals(1, valueFieldStats.size());

        // new antimatter stats were added
        Assert.assertNotNull(keyFieldAntimatterStats);
        Assert.assertEquals(1, keyFieldAntimatterStats.size());
        TestStatisticsEntry keyFieldAntimatterStatsEntry = keyFieldAntimatterStats.iterator().next();
        Assert.assertEquals(keyFieldAntimatterStatsEntry.getComponentId().getMinTimestamp(),
                keyFieldAntimatterStatsEntry.getComponentId().getMaxTimestamp());
        synopsis = (CountingSynopsis) keyFieldAntimatterStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());

        Assert.assertNotNull(valueFieldAntimatterStats);
        Assert.assertEquals(1, valueFieldAntimatterStats.size());
        TestStatisticsEntry valueFieldAntimatterStatsEntry = valueFieldAntimatterStats.iterator().next();
        Assert.assertEquals(valueFieldAntimatterStatsEntry.getComponentId().getMinTimestamp(),
                valueFieldAntimatterStatsEntry.getComponentId().getMaxTimestamp());
        synopsis = (CountingSynopsis) valueFieldAntimatterStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());

        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        // trigger merge and reconciliation resulting in reconciliation of all records
        StorageTestUtils.fullMerge(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET);
        StorageTestUtils.fullMerge(dsLifecycleMgr, secondaryLsmBtree, STATS_DATASET);

        keyFieldStats = testMdProvider.getStats(keyFieldStatisticsID);
        valueFieldStats = testMdProvider.getStats(indexedFieldStatisticsID);
        keyFieldAntimatterStats = testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        valueFieldAntimatterStats = testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        // everything is reconciled
        Assert.assertTrue(keyFieldStats.isEmpty());
        Assert.assertTrue(valueFieldStats.isEmpty());
        Assert.assertTrue(keyFieldAntimatterStats.isEmpty());
        Assert.assertTrue(valueFieldAntimatterStats.isEmpty());
    }

    @Test
    public void testMergeStatisticsExcessAntimatter() throws Exception {
        // create a first component with records [1;500]
        insertRecords(NUM_INSERT_RECORDS / 2);
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsEntry> keyFieldStatsEntries = testMdProvider.getStats(keyFieldStatisticsID);
        Collection<TestStatisticsEntry> valueFieldStatsEntries = testMdProvider.getStats(indexedFieldStatisticsID);
        Collection<TestStatisticsEntry> keyFieldAntimatterStatsEntries =
                testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsEntry> valueFieldAntimatterStatsEntries =
                testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        Assert.assertEquals(1, keyFieldStatsEntries.size());
        TestStatisticsEntry firstKeyFieldStatsEntry = keyFieldStatsEntries.iterator().next();
        CountingSynopsis synopsis = (CountingSynopsis) firstKeyFieldStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());
        Assert.assertEquals(firstKeyFieldStatsEntry.getComponentId().getMinTimestamp(),
                firstKeyFieldStatsEntry.getComponentId().getMaxTimestamp());

        Assert.assertEquals(1, valueFieldStatsEntries.size());
        TestStatisticsEntry firstValueFieldStatsEntry = valueFieldStatsEntries.iterator().next();
        synopsis = (CountingSynopsis) firstValueFieldStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());
        Assert.assertEquals(firstValueFieldStatsEntry.getComponentId().getMinTimestamp(),
                firstValueFieldStatsEntry.getComponentId().getMaxTimestamp());

        Assert.assertTrue(keyFieldAntimatterStatsEntries.isEmpty());
        Assert.assertTrue(valueFieldAntimatterStatsEntries.isEmpty());

        // insert records [501;1000]
        insertRecords(NUM_INSERT_RECORDS / 2);
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);
        ILSMDiskComponent primaryC2 = primaryLsmBtree.getDiskComponents().get(0);
        ILSMDiskComponent secondaryC2 = secondaryLsmBtree.getDiskComponents().get(0);

        keyFieldStatsEntries = testMdProvider.getStats(keyFieldStatisticsID);
        valueFieldStatsEntries = testMdProvider.getStats(indexedFieldStatisticsID);
        keyFieldAntimatterStatsEntries = testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        valueFieldAntimatterStatsEntries = testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        Assert.assertEquals(2, keyFieldStatsEntries.size());
        TestStatisticsEntry secondKeyFieldStatsEntry = Iterators.get(keyFieldStatsEntries.iterator(), 1);
        Assert.assertEquals(secondKeyFieldStatsEntry.getComponentId().getMinTimestamp(),
                secondKeyFieldStatsEntry.getComponentId().getMaxTimestamp());
        synopsis = (CountingSynopsis) secondKeyFieldStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());

        Assert.assertEquals(2, valueFieldStatsEntries.size());
        TestStatisticsEntry secondValueFieldStatsEntry = Iterators.get(valueFieldStatsEntries.iterator(), 1);
        synopsis = (CountingSynopsis) secondValueFieldStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());
        Assert.assertEquals(secondValueFieldStatsEntry.getComponentId().getMinTimestamp(),
                secondValueFieldStatsEntry.getComponentId().getMaxTimestamp());

        Assert.assertTrue(keyFieldAntimatterStatsEntries.isEmpty());
        Assert.assertTrue(valueFieldAntimatterStatsEntries.isEmpty());

        // delete records [1;1000]
        deleteRecords(NUM_INSERT_RECORDS);
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);
        ILSMDiskComponent primaryC3 = primaryLsmBtree.getDiskComponents().get(0);
        ILSMDiskComponent secondaryC3 = secondaryLsmBtree.getDiskComponents().get(0);

        keyFieldStatsEntries = testMdProvider.getStats(keyFieldStatisticsID);
        valueFieldStatsEntries = testMdProvider.getStats(indexedFieldStatisticsID);
        keyFieldAntimatterStatsEntries = testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        valueFieldAntimatterStatsEntries = testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        Assert.assertEquals(2, keyFieldStatsEntries.size());
        Assert.assertEquals(2, valueFieldStatsEntries.size());

        Assert.assertEquals(1, keyFieldAntimatterStatsEntries.size());
        TestStatisticsEntry thirdKeyFieldAntimatterStatsEntry = keyFieldAntimatterStatsEntries.iterator().next();
        Assert.assertEquals(thirdKeyFieldAntimatterStatsEntry.getComponentId().getMinTimestamp(),
                thirdKeyFieldAntimatterStatsEntry.getComponentId().getMaxTimestamp());
        synopsis = (CountingSynopsis) thirdKeyFieldAntimatterStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());

        Assert.assertEquals(1, valueFieldAntimatterStatsEntries.size());
        TestStatisticsEntry thirdValueFieldAntimatterStatsEntry = valueFieldAntimatterStatsEntries.iterator().next();
        Assert.assertEquals(thirdValueFieldAntimatterStatsEntry.getComponentId().getMinTimestamp(),
                thirdValueFieldAntimatterStatsEntry.getComponentId().getMaxTimestamp());
        synopsis = (CountingSynopsis) thirdValueFieldAntimatterStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());

        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        // merge and reconcile records between components C2 & C3, leaving behind component with antimatter [1-500]
        StorageTestUtils.merge(Arrays.asList(primaryC3, primaryC2), dsLifecycleMgr, primaryLsmBtree, STATS_DATASET);
        StorageTestUtils.merge(Arrays.asList(secondaryC3, secondaryC2), dsLifecycleMgr, secondaryLsmBtree,
                STATS_DATASET);

        keyFieldStatsEntries = testMdProvider.getStats(keyFieldStatisticsID);
        valueFieldStatsEntries = testMdProvider.getStats(indexedFieldStatisticsID);
        keyFieldAntimatterStatsEntries = testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        valueFieldAntimatterStatsEntries = testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        Assert.assertEquals(1, keyFieldStatsEntries.size());
        Assert.assertEquals(1, valueFieldStatsEntries.size());
        Assert.assertEquals(1, keyFieldAntimatterStatsEntries.size());
        Assert.assertEquals(1, valueFieldAntimatterStatsEntries.size());

        TestStatisticsEntry mergedKeyComponentStatsEntry = keyFieldAntimatterStatsEntries.iterator().next();
        synopsis = (CountingSynopsis) mergedKeyComponentStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        // TODO: this is a bug, this will generate cardinality NUM_INSERT_RECORDS because rangeCursor returns all
        // deleted records instead of ignoring reconciled ones
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());

        TestStatisticsEntry mergedValueComponentStatsEntry = valueFieldAntimatterStatsEntries.iterator().next();
        synopsis = (CountingSynopsis) mergedValueComponentStatsEntry.getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());

        StorageTestUtils.fullMerge(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET);
        StorageTestUtils.fullMerge(dsLifecycleMgr, secondaryLsmBtree, STATS_DATASET);

        keyFieldStatsEntries = testMdProvider.getStats(keyFieldStatisticsID);
        valueFieldStatsEntries = testMdProvider.getStats(indexedFieldStatisticsID);
        keyFieldAntimatterStatsEntries = testMdProvider.getStats(keyFieldAntimatterStatisticsID);
        valueFieldAntimatterStatsEntries = testMdProvider.getStats(indexedFieldAntimatterStatisticsID);

        Assert.assertTrue(keyFieldStatsEntries.isEmpty());
        Assert.assertTrue(valueFieldStatsEntries.isEmpty());
        Assert.assertTrue(keyFieldAntimatterStatsEntries.isEmpty());
        Assert.assertTrue(valueFieldAntimatterStatsEntries.isEmpty());
    }
}
