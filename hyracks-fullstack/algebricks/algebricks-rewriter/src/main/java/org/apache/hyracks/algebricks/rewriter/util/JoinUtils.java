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
package org.apache.hyracks.algebricks.rewriter.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation.BroadcastSide;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalPropertiesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator.JoinPartitioningType;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HybridHashJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.InMemoryHashJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILogicalPropertiesVector;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;

public class JoinUtils {
    private JoinUtils() {
    }

    public static void setJoinAlgorithmAndExchangeAlgo(AbstractBinaryJoinOperator op, boolean topLevelOp,
            IOptimizationContext context) throws AlgebricksException {
        if (!topLevelOp) {
            throw new IllegalStateException("Micro operator not implemented for: " + op.getOperatorTag());
        }
        List<LogicalVariable> sideLeft = new LinkedList<>();
        List<LogicalVariable> sideRight = new LinkedList<>();
        List<LogicalVariable> varsLeft = op.getInputs().get(0).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(1).getValue().getSchema();
        if (isHashJoinCondition(op.getCondition().getValue(), varsLeft, varsRight, sideLeft, sideRight)) {
            long sizel = getBytes(op.getInputs().get(0).getValue().getInputs().get(0).getValue());
            long sizer = getBytes(op.getInputs().get(1).getValue().getInputs().get(0).getValue());

            long size = 0;
            BroadcastSide side = null;
            if (sizel > 0 && sizel <= 1000000 || sizer > 0 && sizer <= 1000000) {
                if (sizer <= 1000000 && sizel <= 1000000) {
                    if (sizer < sizel) {
                        side = BroadcastSide.RIGHT;
                        size = sizer;
                    } else {
                        side = BroadcastSide.LEFT;
                        size = sizel;
                    }
                } else if (sizer <= 1000000) {
                    side = BroadcastSide.RIGHT;
                    size = sizer;
                } else {
                    side = BroadcastSide.LEFT;
                    size = sizel;
                }
            } else {
                side = getBroadcastJoinSide(op.getCondition().getValue(), varsLeft, varsRight);
            }
            //   BroadcastSide side = getBroadcastJoinSide(op.getCondition().getValue(), varsLeft, varsRight);

            // BroadcastSide side = BroadcastSide.RIGHT;
            if (side == null) {
                setHashJoinOp(op, JoinPartitioningType.PAIRWISE, sideLeft, sideRight, context, size);
            } else {
                switch (side) {
                    case RIGHT:
                        setHashJoinOp(op, JoinPartitioningType.BROADCAST, sideLeft, sideRight, context, size);
                        break;
                    case LEFT:
                        if (op.getJoinKind() == AbstractBinaryJoinOperator.JoinKind.INNER) {
                            Mutable<ILogicalOperator> opRef0 = op.getInputs().get(0);
                            Mutable<ILogicalOperator> opRef1 = op.getInputs().get(1);
                            ILogicalOperator tmp = opRef0.getValue();
                            opRef0.setValue(opRef1.getValue());
                            opRef1.setValue(tmp);
                            setHashJoinOp(op, JoinPartitioningType.BROADCAST, sideRight, sideLeft, context, size);
                        } else {
                            setHashJoinOp(op, JoinPartitioningType.PAIRWISE, sideLeft, sideRight, context, size);
                        }
                        break;
                    default:
                        // This should never happen
                        throw new IllegalStateException(side.toString());
                }
            }
        } else

        {
            setNestedLoopJoinOp(op, context);
        }
    }

    private static void setNestedLoopJoinOp(AbstractBinaryJoinOperator op, IOptimizationContext context) {
        op.setPhysicalOperator(new NestedLoopJoinPOperator(op.getJoinKind(), JoinPartitioningType.BROADCAST,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin()));
    }

    private static void setHashJoinOp(AbstractBinaryJoinOperator op, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context, long size)
            throws AlgebricksException {
        op.setPhysicalOperator(new HybridHashJoinPOperator(op.getJoinKind(), partitioningType, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(),
                context.getPhysicalOptimizationConfig().getMaxFramesForJoinLeftInput(),
                context.getPhysicalOptimizationConfig().getMaxRecordsPerFrame(),
                context.getPhysicalOptimizationConfig().getFudgeFactor()));
        if (partitioningType == JoinPartitioningType.BROADCAST) {
            //hybridToInMemHashJoin(op, context);
            inMemHashJoin(op, 75000);
        }
    }

    private static void hybridToInMemHashJoin(AbstractBinaryJoinOperator op, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator opBuild = op.getInputs().get(1).getValue();
        LogicalPropertiesVisitor.computeLogicalPropertiesDFS(opBuild, context);
        ILogicalPropertiesVector v = context.getLogicalPropertiesVector(opBuild);
        boolean loggerTraceEnabled = AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled();
        if (loggerTraceEnabled) {
            AlgebricksConfig.ALGEBRICKS_LOGGER
                    .trace("// HybridHashJoin inner branch -- Logical properties for " + opBuild + ": " + v + "\n");
        }
        if (v != null) {
            int size2 = v.getMaxOutputFrames();
            HybridHashJoinPOperator hhj = (HybridHashJoinPOperator) op.getPhysicalOperator();
            if (size2 > 0 && size2 * hhj.getFudgeFactor() <= hhj.getMemSizeInFrames()) {
                if (loggerTraceEnabled) {
                    AlgebricksConfig.ALGEBRICKS_LOGGER
                            .trace("// HybridHashJoin inner branch " + opBuild + " fits in memory\n");
                }
                // maintains the local properties on the probe side
                op.setPhysicalOperator(
                        new InMemoryHashJoinPOperator(hhj.getKind(), hhj.getPartitioningType(), hhj.getKeysLeftBranch(),
                                hhj.getKeysRightBranch(), v.getNumberOfTuples() * 2, hhj.getMemSizeInFrames()));
            }
        }

    }

    private static void inMemHashJoin(AbstractBinaryJoinOperator op, long numOfTuples) {
        HybridHashJoinPOperator hhj = (HybridHashJoinPOperator) op.getPhysicalOperator();
        op.setPhysicalOperator(new InMemoryHashJoinPOperator(hhj.getKind(), hhj.getPartitioningType(),
                hhj.getKeysLeftBranch(), hhj.getKeysRightBranch(), (int) numOfTuples * 2, hhj.getMemSizeInFrames()));
    }

    private static long getBytes(ILogicalOperator op) {
        int fields = 0;
        if (op.getCardinality() != null && (op.getOperatorTag() == LogicalOperatorTag.ASSIGN
                || op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN
                || op.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                || op.getOperatorTag() == LogicalOperatorTag.SELECT)) {
            if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                fields = ((DataSourceScanOperator) op.getInputs().get(0).getValue()).getDataSource().fieldsSize();
            } else if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                fields = ((DataSourceScanOperator) op).getDataSource().fieldsSize();
            } else {
                fields = 4;
            }
            return op.getCardinality() * fields * 4;
        } else
            return 0;
    }

    private static boolean isHashJoinCondition(ILogicalExpression e, Collection<LogicalVariable> inLeftAll,
            Collection<LogicalVariable> inRightAll, Collection<LogicalVariable> outLeftFields,
            Collection<LogicalVariable> outRightFields) {
        switch (e.getExpressionTag()) {
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
                FunctionIdentifier fi = fexp.getFunctionIdentifier();
                if (fi.equals(AlgebricksBuiltinFunctions.AND)) {
                    for (Mutable<ILogicalExpression> a : fexp.getArguments()) {
                        if (!isHashJoinCondition(a.getValue(), inLeftAll, inRightAll, outLeftFields, outRightFields)) {
                            return false;
                        }
                    }
                    return true;
                } else {
                    ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(fi);
                    if (ck != ComparisonKind.EQ) {
                        return false;
                    }
                    ILogicalExpression opLeft = fexp.getArguments().get(0).getValue();
                    ILogicalExpression opRight = fexp.getArguments().get(1).getValue();
                    if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
                            || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        return false;
                    }
                    LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
                    if (inLeftAll.contains(var1) && !outLeftFields.contains(var1)) {
                        outLeftFields.add(var1);
                    } else if (inRightAll.contains(var1) && !outRightFields.contains(var1)) {
                        outRightFields.add(var1);
                    } else {
                        return false;
                    }
                    LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
                    if (inLeftAll.contains(var2) && !outLeftFields.contains(var2)) {
                        outLeftFields.add(var2);
                    } else if (inRightAll.contains(var2) && !outRightFields.contains(var2)) {
                        outRightFields.add(var2);
                    } else {
                        return false;
                    }
                    return true;
                }
            }
            default:
                return false;
        }
    }

    private static BroadcastSide getBroadcastJoinSide(ILogicalExpression e, List<LogicalVariable> varsLeft,
            List<LogicalVariable> varsRight) {
        if (e.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (fi.equals(AlgebricksBuiltinFunctions.AND)) {
            BroadcastSide fBcastSide = null;
            for (Mutable<ILogicalExpression> a : fexp.getArguments()) {
                BroadcastSide aBcastSide = getBroadcastJoinSide(a.getValue(), varsLeft, varsRight);
                if (fBcastSide == null) {
                    fBcastSide = aBcastSide;
                } else if (aBcastSide != null && !aBcastSide.equals(fBcastSide)) {
                    return null;
                }
            }
            return fBcastSide;
        } else {
            IExpressionAnnotation ann =
                    fexp.getAnnotations().get(BroadcastExpressionAnnotation.BROADCAST_ANNOTATION_KEY);
            if (ann == null) {
                return null;
            }
            BroadcastSide side = (BroadcastSide) ann.getObject();
            if (side == null) {
                return null;
            }
            int i;
            switch (side) {
                case LEFT:
                    i = 0;
                    break;
                case RIGHT:
                    i = 1;
                    break;
                default:
                    return null;
            }
            ArrayList<LogicalVariable> vars = new ArrayList<>();
            fexp.getArguments().get(i).getValue().getUsedVariables(vars);
            if (varsLeft.containsAll(vars)) {
                return BroadcastSide.LEFT;
            } else if (varsRight.containsAll(vars)) {
                return BroadcastSide.RIGHT;
            } else {
                return null;
            }
        }
    }
}
