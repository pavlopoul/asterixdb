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
package org.apache.hyracks.algebricks.compiler.api;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.PlanCompiler;
import org.apache.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import org.apache.hyracks.algebricks.core.rewriter.base.IOptimizationContextFactory;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobSpecification;

public class HeuristicCompilerFactoryBuilder extends AbstractCompilerFactoryBuilder {

    public static class DefaultOptimizationContextFactory implements IOptimizationContextFactory {

        public static final DefaultOptimizationContextFactory INSTANCE = new DefaultOptimizationContextFactory();

        private DefaultOptimizationContextFactory() {
        }

        @Override
        public IOptimizationContext createOptimizationContext(int varCounter,
                IExpressionEvalSizeComputer expressionEvalSizeComputer,
                IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
                IExpressionTypeComputer expressionTypeComputer, IMissableTypeComputer missableTypeComputer,
                IConflictingTypeResolver conflictingTypeResolver, PhysicalOptimizationConfig physicalOptimizationConfig,
                AlgebricksPartitionConstraint clusterLocations) {
            LogicalOperatorPrettyPrintVisitor prettyPrintVisitor = new LogicalOperatorPrettyPrintVisitor();
            return new AlgebricksOptimizationContext(varCounter, expressionEvalSizeComputer,
                    mergeAggregationExpressionFactory, expressionTypeComputer, missableTypeComputer,
                    conflictingTypeResolver, physicalOptimizationConfig, clusterLocations, prettyPrintVisitor);
        }
    }

    private IOptimizationContextFactory optCtxFactory;

    public JobGenContext context;
    public PlanCompiler pc;

    public HeuristicCompilerFactoryBuilder() {
        this.optCtxFactory = DefaultOptimizationContextFactory.INSTANCE;
    }

    public HeuristicCompilerFactoryBuilder(IOptimizationContextFactory optCtxFactory) {
        this.optCtxFactory = optCtxFactory;
    }

    @Override
    public ICompilerFactory create() {
        return new ICompilerFactory() {
            @Override
            public ICompiler createCompiler(final ILogicalPlan plan, final IMetadataProvider<?, ?> metadata,
                    int varCounter) {
                final IOptimizationContext oc = optCtxFactory.createOptimizationContext(varCounter,
                        expressionEvalSizeComputer, mergeAggregationExpressionFactory, expressionTypeComputer,
                        missableTypeComputer, conflictingTypeResolver, physicalOptimizationConfig, clusterLocations);
                oc.setMetadataDeclarations(metadata);
                final HeuristicOptimizer opt = new HeuristicOptimizer(plan, logicalRewrites, physicalRewrites, oc);
                return new ICompiler() {

                    @Override
                    public void optimize() throws AlgebricksException {
                        opt.optimize();
                    }

                    @Override
                    public JobSpecification createJob(Object appContext,
                            IJobletEventListenerFactory jobEventListenerFactory, List<ILogicalOperator> operators,
                            boolean first,
                            Map<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> operatorVisitedToParents,
                            JobGenContext context1, PlanCompiler pc1) throws AlgebricksException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting Job Generation.\n");
                        //                        if (first) {
                        //                            context = new JobGenContext(null, metadata, appContext, serializerDeserializerProvider,
                        //                                    hashFunctionFactoryProvider, hashFunctionFamilyProvider, comparatorFactoryProvider,
                        //                                    typeTraitProvider, binaryBooleanInspectorFactory, binaryIntegerInspectorFactory,
                        //                                    printerProvider, missingWriterFactory, normalizedKeyComputerFactoryProvider,
                        //                                    expressionRuntimeProvider, expressionTypeComputer, oc, expressionEvalSizeComputer,
                        //                                    partialAggregationTypeComputer, predEvaluatorFactoryProvider,
                        //                                    physicalOptimizationConfig.getFrameSize(), clusterLocations);
                        //                            pc = new PlanCompiler(context);
                        //                        } else {
                        if (!first) {
                            context = context1;
                            pc = pc1;
                        }

                        //                        }
                        //pc = new PlanCompiler(context);
                        return pc.compilePlan(plan, jobEventListenerFactory, operators, first,
                                operatorVisitedToParents);
                    }

                    @Override
                    public List<ILogicalOperator> getOperators() throws AlgebricksException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting Job Generation.\n");
                        return pc.getOperators();
                    }

                    @Override
                    public Map<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> getParentOperators()
                            throws AlgebricksException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting Job Generation.\n");
                        return pc.getParentOperator();
                    }

                    @Override
                    public JobGenContext getContext() throws AlgebricksException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting Job Generation.\n");
                        return context;
                    }

                    @Override
                    public PlanCompiler getCompiler() throws AlgebricksException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting Job Generation.\n");
                        return pc;
                    }

                    @Override
                    public boolean getFinished(Object appContext, boolean first, JobGenContext context1,
                            PlanCompiler pc1) throws AlgebricksException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting Job Generation.\n");
                        //                        if (first) {
                        //                            context = new JobGenContext(null, metadata, appContext, serializerDeserializerProvider,
                        //                                    hashFunctionFactoryProvider, hashFunctionFamilyProvider, comparatorFactoryProvider,
                        //                                    typeTraitProvider, binaryBooleanInspectorFactory, binaryIntegerInspectorFactory,
                        //                                    printerProvider, missingWriterFactory, normalizedKeyComputerFactoryProvider,
                        //                                    expressionRuntimeProvider, expressionTypeComputer, oc, expressionEvalSizeComputer,
                        //                                    partialAggregationTypeComputer, predEvaluatorFactoryProvider,
                        //                                    physicalOptimizationConfig.getFrameSize(), clusterLocations);
                        //                            pc = new PlanCompiler(context);
                        //                        } else {
                        //                            context = context1;
                        //                            pc = pc1;
                        //                        }

                        return pc.getFinished();
                    }

                    @Override
                    public List<ILogicalOperator> traversePlan(Object appContext, boolean first, JobGenContext context1,
                            PlanCompiler pc1) throws AlgebricksException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting Job Generation.\n");
                        // if (first) {
                        context = new JobGenContext(null, metadata, appContext, serializerDeserializerProvider,
                                hashFunctionFactoryProvider, hashFunctionFamilyProvider, comparatorFactoryProvider,
                                typeTraitProvider, binaryBooleanInspectorFactory, binaryIntegerInspectorFactory,
                                printerProvider, missingWriterFactory, normalizedKeyComputerFactoryProvider,
                                expressionRuntimeProvider, expressionTypeComputer, oc, expressionEvalSizeComputer,
                                partialAggregationTypeComputer, predEvaluatorFactoryProvider,
                                physicalOptimizationConfig.getFrameSize(), clusterLocations);
                        pc = new PlanCompiler(context);
                        //                        } else {
                        //                            context = context1;
                        //                            pc = pc1;
                        //                        }
                        return pc.traversePlan(plan);
                    }

                    @Override
                    public JobSpecification createLoadJob(Object appContext,
                            IJobletEventListenerFactory jobEventListenerFactory) throws AlgebricksException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting Job Generation.\n");
                        JobGenContext context = new JobGenContext(null, metadata, appContext,
                                serializerDeserializerProvider, hashFunctionFactoryProvider, hashFunctionFamilyProvider,
                                comparatorFactoryProvider, typeTraitProvider, binaryBooleanInspectorFactory,
                                binaryIntegerInspectorFactory, printerProvider, missingWriterFactory,
                                normalizedKeyComputerFactoryProvider, expressionRuntimeProvider, expressionTypeComputer,
                                oc, expressionEvalSizeComputer, partialAggregationTypeComputer,
                                predEvaluatorFactoryProvider, physicalOptimizationConfig.getFrameSize(),
                                clusterLocations);

                        PlanCompiler pc = new PlanCompiler(context);
                        return pc.compileLoadPlan(plan, jobEventListenerFactory);
                    }
                };
            }
        };
    }

}
