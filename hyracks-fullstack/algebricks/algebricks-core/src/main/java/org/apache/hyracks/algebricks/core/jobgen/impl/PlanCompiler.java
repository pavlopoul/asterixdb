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
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.misc.SinkOperatorDescriptor;

public class PlanCompiler {
    public static final String[] ASTERIX_IDS =
            { "asterix-001", "asterix-002", "asterix-003", "asterix-004", "asterix-005", "asterix-006", "asterix-007" };
    private JobGenContext context;
    private JobSpecification spec;
    private JobBuilder builder;
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

    public JobSpecification compilePlan(JobSpecification spec1, JobBuilder builder1, ILogicalPlan plan,
            IJobletEventListenerFactory jobEventListenerFactory, List<ILogicalOperator> operators2, boolean first,
            Map<Mutable<ILogicalOperator>, List<ILogicalOperator>> operatorVisitedToParents2)
            throws AlgebricksException {
        if (!first) {
            operatorVisitedToParents = operatorVisitedToParents2;
        }
        return compilePlanImpl(spec1, builder1, plan, false, null, jobEventListenerFactory, operators2, first);
    }

    public JobSpecification compileNestedPlan(JobSpecification spec1, JobBuilder builder1, ILogicalPlan plan,
            IOperatorSchema outerPlanSchema) throws AlgebricksException {
        return compilePlanImpl(spec1, builder1, plan, true, outerPlanSchema, null, null, true);
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

    private JobSpecification compilePlanImpl(JobSpecification spec1, JobBuilder builder1, ILogicalPlan plan,
            boolean isNestedPlan, IOperatorSchema outerPlanSchema, IJobletEventListenerFactory jobEventListenerFactory,
            List<ILogicalOperator> operators2, boolean first) throws AlgebricksException {
        if (first) {
            spec = new JobSpecification(context.getFrameSize());
        } else {
            spec = spec1;
        }
        //JobSpecification spec = new JobSpecification(context.getFrameSize());
        if (jobEventListenerFactory != null) {
            spec.setJobletEventListenerFactory(jobEventListenerFactory);
        }
        List<ILogicalOperator> rootOps = new ArrayList<>();
        if (first) {
            builder = new JobBuilder(spec, context.getClusterLocations());
        } else {
            builder = builder1;
        }
        //JobBuilder builder = new JobBuilder(spec, context.getClusterLocations());
        Mutable<ILogicalOperator> opRef = plan.getRoots().get(0);
        operators = operators2;
        //compileOpRef(opRef, spec, builder, outerPlanSchema);
        //rootOps.add(opRef.getValue());

        rootOps.add(compileOpRef(opRef, spec, builder, outerPlanSchema));
        reviseEdges(builder);
        //operatorVisitedToParents.clear();
        SinkOperatorDescriptor sink = new SinkOperatorDescriptor(spec, 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sink, ASTERIX_IDS);
        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        IOperatorDescriptor source = spec.getOperatorMap().values().stream().findFirst().get();
        spec.connect(conn, source, 0, sink, 0);
        spec.addRoot(sink);
        // spec.addRoot(new SinkOperatorDescriptor(spec, 1));
        builder.buildSpec(rootOps);
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

    public JobSpecification getSpec() {
        return spec;
    }

    public JobBuilder getBuilder() {
        return builder;
    }

    public Map<Mutable<ILogicalOperator>, List<ILogicalOperator>> getParentOperator() {
        return operatorVisitedToParents;
    }

    public List<ILogicalOperator> traversePlan(ILogicalPlan plan) {
        Mutable<ILogicalOperator> opRef = plan.getRoots().get(0);
        ILogicalOperator op = opRef.getValue();
        while (op.hasInputs()) {
            operators.add(op);
            op = op.getInputs().get(0).getValue();
        }
        operators.add(op);
        return operators;
    }

    private ILogicalOperator compileOpRef(Mutable<ILogicalOperator> opRef, IOperatorDescriptorRegistry spec,
            IHyracksJobBuilder builder, IOperatorSchema outerPlanSchema) throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        for (int j = 0; j < operators.size(); j++) {
            //            int n = op.getInputs().size();
            //            IOperatorSchema[] schemas = new IOperatorSchema[n];
            //            int i = 0;
            int n = 0;
            IOperatorSchema[] schemas = new IOperatorSchema[n];
            int i = 0;
            if (j == operators.size() - 1 || (operators.get(j + 1).getOperatorTag() == LogicalOperatorTag.EXCHANGE
                    && j == operators.size() - 3)) {
                if (operators.get(j + 1).getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                    n = op.getInputs().size();
                    schemas = new IOperatorSchema[n];
                    i = 0;
                    if (operators.get(j + 2).hasInputs()) {

                        schemas[i++] =
                                context.getSchema(operators.get(operators.size() - 1).getInputs().get(0).getValue());
                    }
                    createSchema(operators.get(j + 2), schemas, outerPlanSchema);
                    operators.remove(operators.size() - 1);
                    schemas = new IOperatorSchema[n];
                    schemas[0] = context.getSchema(operators.get(operators.size() - 1).getInputs().get(0).getValue());
                    //                }
                    createSchema(operators.get(j + 1), schemas, outerPlanSchema);
                    operators.remove(operators.size() - 1);
                }
                //                if (op.hasInputs()) {
                schemas = new IOperatorSchema[n];
                schemas[0] = context.getSchema(operators.get(operators.size() - 1).getInputs().get(0).getValue());
                IOperatorSchema opSchema = new OperatorSchemaImpl();
                context.putSchema(operators.get(operators.size() - 1), opSchema);
                operators.get(operators.size() - 1).getVariablePropagationPolicy().propagateVariables(opSchema,
                        schemas);
                operators.get(operators.size() - 1).contributeRuntimeOperator(builder, context, opSchema, schemas,
                        outerPlanSchema);
                op = operators.get(operators.size() - 1);
                operators.remove(operators.size() - 1);
            } else {
                Mutable<ILogicalOperator> opChild = operators.get(j).getInputs().get(0);
                List<ILogicalOperator> parents = operatorVisitedToParents.get(opChild);
                if (parents == null) {
                    parents = new ArrayList<ILogicalOperator>();
                    operatorVisitedToParents.put(opChild, parents);
                    parents.add(operators.get(j));
                    op = opChild.getValue();
                } else {
                    if (!parents.contains(operators.get(j)))
                        parents.add(operators.get(j));
                    op = opChild.getValue();
                    continue;
                }

            }
        }
        if (operators.isEmpty())
            finished = true;
        return op;
    }

    public void createSchema(ILogicalOperator op, IOperatorSchema[] schemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
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
