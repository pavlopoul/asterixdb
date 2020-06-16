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
package org.apache.hyracks.algebricks.core.jobgen.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobSpecification;

public class PlanCompiler {
    private JobGenContext context;
    private List<ILogicalOperator> operators = new ArrayList<>();
    private boolean finished = false;
    private Map<Mutable<ILogicalOperator>, List<ILogicalOperator>> operatorVisitedToParents =
            new HashMap<Mutable<ILogicalOperator>, List<ILogicalOperator>>();

    public PlanCompiler(JobGenContext context) {
        this.context = context;
    }

    public JobGenContext getContext() {
        return context;
    }

    public JobSpecification compileLoadPlan(ILogicalPlan plan, IJobletEventListenerFactory jobEventListenerFactory)
            throws AlgebricksException {
        return compileLoadPlanImpl(plan, false, null, jobEventListenerFactory);
    }

    public JobSpecification compilePlan(ILogicalPlan plan, IJobletEventListenerFactory jobEventListenerFactory,
            List<ILogicalOperator> operators2, boolean first, boolean notJoinInPlan) throws AlgebricksException {
        return compilePlanImpl(plan, false, null, jobEventListenerFactory, operators2, first, notJoinInPlan);
    }

    public JobSpecification compileNestedPlan(ILogicalPlan plan, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        return compilePlanImpl(plan, true, outerPlanSchema, null, null, true, true);
    }

    private JobSpecification compileLoadPlanImpl(ILogicalPlan plan, boolean isNestedPlan,
            IOperatorSchema outerPlanSchema, IJobletEventListenerFactory jobEventListenerFactory)
            throws AlgebricksException {
        JobSpecification spec = new JobSpecification(context.getFrameSize());
        if (jobEventListenerFactory != null) {
            spec.setJobletEventListenerFactory(jobEventListenerFactory);
        }
        List<ILogicalOperator> rootOps = new ArrayList<>();
        JobBuilder builder = new JobBuilder(spec, context.getClusterLocations());
        for (Mutable<ILogicalOperator> opRef : plan.getRoots()) {
            compileLoadOpRef(opRef, spec, builder, outerPlanSchema);
            rootOps.add(opRef.getValue());
        }
        reviseEdges(builder);
        operatorVisitedToParents.clear();
        builder.buildSpec(rootOps);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        // Do not do activity cluster planning because it is slow on large clusters
        spec.setUseConnectorPolicyForScheduling(false);
        if (isNestedPlan) {
            spec.setMetaOps(builder.getGeneratedMetaOps());
        }
        return spec;
    }

    private void compileLoadOpRef(Mutable<ILogicalOperator> opRef, IOperatorDescriptorRegistry spec,
            IHyracksJobBuilder builder, IOperatorSchema outerPlanSchema) throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        int n = op.getInputs().size();
        IOperatorSchema[] schemas = new IOperatorSchema[n];
        int i = 0;
        for (Mutable<ILogicalOperator> opChild : op.getInputs()) {
            List<ILogicalOperator> parents = operatorVisitedToParents.get(opChild);
            if (parents == null) {
                parents = new ArrayList<ILogicalOperator>();
                operatorVisitedToParents.put(opChild, parents);
                parents.add(opRef.getValue());
                compileLoadOpRef(opChild, spec, builder, outerPlanSchema);
                schemas[i++] = context.getSchema(opChild.getValue());
            } else {
                if (!parents.contains(opRef.getValue()))
                    parents.add(opRef.getValue());
                schemas[i++] = context.getSchema(opChild.getValue());
                continue;
            }
        }

        IOperatorSchema opSchema = new OperatorSchemaImpl();
        context.putSchema(op, opSchema);
        op.getVariablePropagationPolicy().propagateVariables(opSchema, schemas);
        op.contributeRuntimeOperator(builder, context, opSchema, schemas, outerPlanSchema);
    }

    private JobSpecification compilePlanImpl(ILogicalPlan plan, boolean isNestedPlan, IOperatorSchema outerPlanSchema,
            IJobletEventListenerFactory jobEventListenerFactory, List<ILogicalOperator> operators2, boolean first,
            boolean notJoinInPlan) throws AlgebricksException {
        JobSpecification spec = new JobSpecification(context.getFrameSize());
        if (jobEventListenerFactory != null) {
            spec.setJobletEventListenerFactory(jobEventListenerFactory);
        }
        List<ILogicalOperator> rootOps = new ArrayList<>();
        JobBuilder builder = new JobBuilder(spec, context.getClusterLocations());
        Mutable<ILogicalOperator> opRef = plan.getRoots().get(0);
        operators = operators2;
        rootOps.add(opRef.getValue());

        compileOpRef(spec, builder, outerPlanSchema, first, notJoinInPlan);
        if (!notJoinInPlan) {
            builder.buildSpecNew();
        } else {
            builder.buildSpec(rootOps);
        }

        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        // Do not do activity cluster planning because it is slow on large clusters
        spec.setUseConnectorPolicyForScheduling(false);
        if (isNestedPlan) {
            spec.setMetaOps(builder.getGeneratedMetaOps());
        }
        return spec;
    }

    public boolean getFinished() {
        return finished;
    }

    public List<ILogicalOperator> getOperators() {
        return operators;
    }

    public List<ILogicalOperator> traversePlan(Mutable<ILogicalOperator> root, boolean rootFlag) {
        if (rootFlag) {
            operators.add(root.getValue());
        }
        ILogicalOperator op = root.getValue();
        for (Mutable<ILogicalOperator> opChild : op.getInputs()) {
            operators.add(opChild.getValue());
            traversePlan(opChild, false);
        }
        return operators;
    }

    private void compileOpRef(IOperatorDescriptorRegistry spec, IHyracksJobBuilder builder,
            IOperatorSchema outerPlanSchema, boolean first, boolean notJoinInPlan) throws AlgebricksException {
        int size = operators.size();
        for (int j = operators.size() - 1; j >= 0; j--) {
            int n = operators.get(j).getInputs().size();
            int i = 0;
            IOperatorSchema[] schemas = new IOperatorSchema[n];
            if (!notJoinInPlan || onlySelect()) {
                if (operators.get(j).hasInputs()) {
                    if (operators.get(j).getOperatorTag() == LogicalOperatorTag.DISTRIBUTE_RESULT) {
                        break;
                    }
                }
            }
            if (j != size - 1) {
                for (Mutable<ILogicalOperator> opChild : operators.get(j).getInputs())
                    schemas[i++] = context.getSchema(opChild.getValue());
            }
            createSchema(operators.get(j), schemas, outerPlanSchema, builder);

        }
        //        if (notJoinInPlan()) {
        //            finished = true;
        //        }
        return;

    }

    private boolean onlySelect() {
        int joins = 0;
        for (ILogicalOperator op : operators) {
            if (op.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                joins++;
                //return false;
            }
        }
        if (joins > 0) {
            return false;
        }
        return true;
    }

    public void createSchema(ILogicalOperator op, IOperatorSchema[] schemas, IOperatorSchema outerPlanSchema,
            IHyracksJobBuilder builder) throws AlgebricksException {
        IOperatorSchema opSchema = new OperatorSchemaImpl();
        context.putSchema(op, opSchema);
        op.getVariablePropagationPolicy().propagateVariables(opSchema, schemas);
        op.contributeRuntimeOperator(builder, context, opSchema, schemas, outerPlanSchema);
    }

    private void reviseEdges(IHyracksJobBuilder builder) {
        /*
         * revise the edges for the case of replicate operator
         */
        operatorVisitedToParents.forEach((child, parents) -> {
            if (parents.size() > 1) {
                if (child.getValue().getOperatorTag() == LogicalOperatorTag.REPLICATE
                        || child.getValue().getOperatorTag() == LogicalOperatorTag.SPLIT) {
                    AbstractReplicateOperator rop = (AbstractReplicateOperator) child.getValue();
                    if (rop.isBlocker()) {
                        // make the order of the graph edges consistent with the order of rop's outputs
                        List<Mutable<ILogicalOperator>> outputs = rop.getOutputs();
                        for (ILogicalOperator parent : parents) {
                            builder.contributeGraphEdge(child.getValue(), outputs.indexOf(parent), parent, 0);
                        }
                    } else {
                        int i = 0;
                        for (ILogicalOperator parent : parents) {
                            builder.contributeGraphEdge(child.getValue(), i, parent, 0);
                            i++;
                        }
                    }
                }
            }
        });
    }
}
