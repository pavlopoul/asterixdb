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
package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AIntegerObject;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.am.BTreeAccessMethod.LimitType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.CardinalityInferenceVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class CostBasedRule implements IAlgebraicRewriteRule {

    private Map<AbstractLogicalOperator, List<JoinNode>> joinMap = new HashMap<>();

    private AbstractLogicalOperator alo, sOp, lOp, parentAssign;
    private List<JoinNode> joins = new ArrayList<>();
    private List<InnerJoinOperator> minJoins = new ArrayList<>();

    protected boolean checkAndReturnExpr(AbstractLogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        // First check that the operator is a select or join and its condition is a function call.
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        if (op.hasInputs()) {
            if (op.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                alo = op;
                ILogicalOperator childop = op.getInputs().get(0).getValue();
                if (childop.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                    if (childop.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.INNERJOIN
                            && childop.getInputs().get(1).getValue().getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
                        return false;
                    }
                    DatasetDataSource datasourcer = null;
                    AssignOperator assignr = null;
                    boolean only2joins = false;
                    if (childop.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                        InnerJoinOperator inn = (InnerJoinOperator) childop.getInputs().get(0).getValue();
                        if (inn.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.INNERJOIN
                                && inn.getInputs().get(1).getValue().getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
                            only2joins = true;
                        }
                    } else if (childop.getInputs().get(1).getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                        InnerJoinOperator inn = (InnerJoinOperator) childop.getInputs().get(0).getValue();
                        if (inn.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.INNERJOIN
                                && inn.getInputs().get(1).getValue().getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
                            only2joins = true;
                        }
                    }
                    populateMap(op.getInputs(), datasourcer, assignr, context, only2joins);
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    public void populateMap(List<Mutable<ILogicalOperator>> inputs, DatasetDataSource datasourcer,
            AssignOperator assignr, IOptimizationContext context, boolean only2joins) throws AlgebricksException {
        for (Mutable<ILogicalOperator> child : inputs) {
            if (child.getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                InnerJoinOperator join = (InnerJoinOperator) child.getValue();
                Mutable<ILogicalExpression> condition = join.getCondition();
                ScalarFunctionCallExpression sfce = (ScalarFunctionCallExpression) condition.getValue();

                LogicalVariable lvl =
                        ((VariableReferenceExpression) sfce.getArguments().get(0).getValue()).getVariableReference();

                AbstractLogicalOperator inputA = findDataSourceOp(null, join, lvl);
                String fieldA = findFieldName(inputA, lvl);
                DatasetDataSource datasourcel = findDataSource(inputA);
                LogicalVariable lvr =
                        ((VariableReferenceExpression) sfce.getArguments().get(1).getValue()).getVariableReference();
                AbstractLogicalOperator A = inputA;
                if (parentAssign != null) {
                    A = parentAssign;
                }
                AbstractLogicalOperator inputB = findDataSourceOp(null, join, lvr);
                String fieldB = findFieldName(inputB, lvr);
                datasourcer = findDataSource(inputB);
                AbstractLogicalOperator B = inputB;
                if (parentAssign != null) {
                    B = parentAssign;
                }

                if (join.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.SELECT
                        || join.getInputs().get(1).getValue().getOperatorTag() == LogicalOperatorTag.SELECT) {
                    DatasetDataSource selectds;
                    if (join.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.SELECT) {
                        selectds = datasourcel;
                        A = (AbstractLogicalOperator) join.getInputs().get(0).getValue();
                        A.setCardinality(inferSelectCardinality(A, context, selectds,
                                (AssignOperator) A.getInputs().get(0).getValue()));
                    } else {
                        selectds = datasourcer;
                        B = (AbstractLogicalOperator) join.getInputs().get(1).getValue();
                        B.setCardinality(inferSelectCardinality(B, context, selectds,
                                (AssignOperator) B.getInputs().get(0).getValue()));
                    }

                }
                long key = inferCardinality((AbstractLogicalOperator) child.getValue(), context, datasourcel,
                        datasourcer, A, B, fieldA, fieldB);
                child.getValue().setCardinality(key);

                JoinNode jnode = new JoinNode((InnerJoinOperator) child.getValue(), A, B);
                if (!joinMap.containsKey(A)) {
                    joinMap.put(A, new ArrayList<>());
                }
                joinMap.get(A).add(jnode);

                if (!joinMap.containsKey(B)) {
                    joinMap.put(B, new ArrayList<>());
                }
                joinMap.get(B).add(jnode);
                joins.add(jnode);

                System.out.println(key);
            }
            populateMap(child.getValue().getInputs(), datasourcer, assignr, context, only2joins);

        }
    }

    private String findFieldName(AbstractLogicalOperator op, LogicalVariable lv) {
        String fieldName = "";
        DataSourceScanOperator scan = null;
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assign = (AssignOperator) op;
            scan = (DataSourceScanOperator) assign.getInputs().get(0).getValue();
            int i = -1;
            for (LogicalVariable var : assign.getVariables()) {
                i++;
                if (var == lv) {
                    ScalarFunctionCallExpression sfce =
                            (ScalarFunctionCallExpression) assign.getExpressions().get(i).getValue();
                    ConstantExpression ce = (ConstantExpression) sfce.getArguments().get(1).getValue();
                    AsterixConstantValue acv = (AsterixConstantValue) ce.getValue();
                    AInt32 aint32 = (AInt32) acv.getObject();
                    int field = aint32.getIntegerValue();
                    fieldName = ((ARecordType) ((DatasetDataSource) scan.getDataSource()).getItemType())
                            .getFieldNames()[field];
                }
            }
        } else {
            scan = (DataSourceScanOperator) op;
            int i = -1;
            for (LogicalVariable var : scan.getVariables()) {
                i++;
                if (lv == var) {
                    fieldName = ((InternalDatasetDetails) ((DatasetDataSource) scan.getDataSource()).getDataset()
                            .getDatasetDetails()).getPrimaryKey().get(i).get(0);
                }
            }
        }

        return fieldName;
    }

    public AbstractLogicalOperator findDataSourceOp(AbstractLogicalOperator parent, AbstractLogicalOperator op,
            LogicalVariable lv) {
        AbstractLogicalOperator out = null;

        AssignOperator assign = null;
        DataSourceScanOperator scan = null;
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            assign = (AssignOperator) op;
            for (LogicalVariable assignVar : assign.getVariables()) {
                if (lv == assignVar) {
                    parentAssign = null;
                    return assign;
                }
            }
        }
        if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            scan = (DataSourceScanOperator) op;
            for (LogicalVariable scanVar : scan.getVariables()) {
                if (lv == scanVar) {
                    if (parent instanceof AssignOperator) {
                        parentAssign = parent;
                    } else {
                        parentAssign = null;
                    }
                    return scan;
                }
            }
        }

        for (Mutable<ILogicalOperator> child : op.getInputs()) {
            out = findDataSourceOp(op, (AbstractLogicalOperator) child.getValue(), lv);
            if (out != null)
                break;
        }
        return out;

    }

    public DatasetDataSource findDataSource(AbstractLogicalOperator op) {
        DataSourceScanOperator scan = null;
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assign = (AssignOperator) op;
            scan = (DataSourceScanOperator) assign.getInputs().get(0).getValue();
        } else if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            scan = (DataSourceScanOperator) op;
        }
        return scan != null ? (DatasetDataSource) scan.getDataSource() : null;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (checkAndReturnExpr(op, context)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        int threshold = joins.size();
        if (threshold <= 1) {
            return false;
        }

        while (threshold > 0) {
            long min = Long.MAX_VALUE;
            JoinNode selectedJoin = null;
            for (JoinNode join : joins) {
                if (join.join.getCardinality() < min) {
                    selectedJoin = join;
                    min = join.join.getCardinality();
                }
            }

            InnerJoinOperator newJoin = new InnerJoinOperator(selectedJoin.join.getCondition(),
                    new MutableObject<ILogicalOperator>(selectedJoin.inputA),
                    new MutableObject<ILogicalOperator>(selectedJoin.inputB));

            context.computeAndSetTypeEnvironmentForOperator(newJoin);
            minJoins.add(newJoin);

            joins.remove(selectedJoin);
            joinMap.get(selectedJoin.inputA).remove(selectedJoin);
            joinMap.get(selectedJoin.inputB).remove(selectedJoin);

            List<JoinNode> adjA = joinMap.get(selectedJoin.inputA);
            List<JoinNode> adjB = joinMap.get(selectedJoin.inputB);

            for (JoinNode jn : adjA) {
                fixCardinality(jn, selectedJoin, "A");
            }

            for (JoinNode jn : adjB) {
                fixCardinality(jn, selectedJoin, "B");
            }
            threshold--;
        }
        InnerJoinOperator searchJ = null;
        for (int i = 0; i < minJoins.size() - 1; i++) {
            InnerJoinOperator minJoin = minJoins.get(i);

            AbstractLogicalOperator left = (AbstractLogicalOperator) minJoin.getInputs().get(0).getValue();
            DatasetDataSource leftD = findDataSource(left);

            AbstractLogicalOperator right = (AbstractLogicalOperator) minJoin.getInputs().get(1).getValue();
            DatasetDataSource rightD = findDataSource(right);

            for (int j = i + 1; j < minJoins.size(); j++) {
                searchJ = minJoins.get(j);

                AbstractLogicalOperator searchL = (AbstractLogicalOperator) searchJ.getInputs().get(0).getValue();
                DatasetDataSource leftS = findDataSource(searchL);

                AbstractLogicalOperator searchR = (AbstractLogicalOperator) searchJ.getInputs().get(1).getValue();
                DatasetDataSource rightS = findDataSource(searchR);

                if (left.equals(searchL) || right.equals(searchL)) {
                    searchJ.getInputs().set(0, new MutableObject<ILogicalOperator>(minJoin));
                } else if (left.equals(searchR) || right.equals(searchR)) {
                    searchJ.getInputs().set(1, new MutableObject<ILogicalOperator>(minJoin));
                } else if ((leftD != null || rightD != null) && leftS != null) {
                    if ((leftD != null && leftD.equals(leftS)) || (rightD != null && rightD.equals(leftS))) {
                        searchJ.getInputs().set(0, new MutableObject<ILogicalOperator>(minJoin));
                    }
                } else if ((leftD != null || rightD != null) && rightS != null) {
                    if ((leftD != null && leftD.equals(rightS)) || (rightD != null && rightD.equals(rightS))) {
                        searchJ.getInputs().set(1, new MutableObject<ILogicalOperator>(minJoin));
                    }
                }
                context.computeAndSetTypeEnvironmentForOperator(searchJ);
            }
        }
        alo.getInputs().clear();
        alo.getInputs().add(new MutableObject<ILogicalOperator>(searchJ));
        return true;

    }

    private void fixCardinality(JoinNode jn, JoinNode selectedJoin, String side) {
        long cardinality = jn.join.getCardinality();
        if (side.equals("A")) {
            if (jn.inputA.equals(selectedJoin.inputA)) {
                cardinality = (selectedJoin.join.getCardinality() * cardinality) / jn.inputA.getCardinality();
            } else if (jn.inputB.equals(selectedJoin.inputA)) {
                cardinality = (selectedJoin.join.getCardinality() * cardinality) / jn.inputB.getCardinality();
            }
        } else {
            if (jn.inputA.equals(selectedJoin.inputB)) {
                cardinality = (selectedJoin.join.getCardinality() * cardinality) / jn.inputA.getCardinality();
            } else if (jn.inputB.equals(selectedJoin.inputB)) {
                cardinality = (selectedJoin.join.getCardinality() * cardinality) / jn.inputB.getCardinality();
            }
        }

        cardinality = Math.max(cardinality, 1);
        //        cardinality = (long) temp * selectedJoin.join.getCardinality();
        joins.remove(jn);
        jn.join.setCardinality(cardinality);
        joins.add(jn);
    }

    public LogicalVariable findSelect(ILogicalOperator op) {
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            Mutable<ILogicalExpression> condition = ((SelectOperator) op).getCondition();

            ScalarFunctionCallExpression sfce = (ScalarFunctionCallExpression) condition.getValue();
            LogicalVariable lvl =
                    ((VariableReferenceExpression) sfce.getArguments().get(0).getValue()).getVariableReference();
            return lvl;
        }
        LogicalVariable lv = null;
        for (Mutable<ILogicalOperator> opChild : op.getInputs()) {
            lv = findSelect(opChild.getValue());
        }
        return lv;
    }

    private long inferCardinality(AbstractLogicalOperator op, IOptimizationContext context,
            DatasetDataSource datasourcel, DatasetDataSource datasourcer, AbstractLogicalOperator inputA,
            AbstractLogicalOperator inputB, String leftField, String rightField) throws AlgebricksException {
        context.addToDontApplySet(this, op);

        if (leftField != null && rightField != null) {
            List<String> leftFields = new ArrayList<>();
            leftFields.add(leftField);
            List<String> rightFields = new ArrayList<>();
            rightFields.add(rightField);
            System.out.println(
                    datasourcel.getDataset().getDatasetName() + " " + datasourcer.getDataset().getDatasetName());
            if (inputA.getOperatorTag() != LogicalOperatorTag.SELECT) {
                inputA.setCardinality(context.getCardinalityEstimator().getTableCardinality(
                        context.getMetadataProvider(), datasourcel.getDataset().getDataverseName(),
                        datasourcel.getDataset().getDatasetName(), leftFields));
            } else {
                return context.getCardinalityEstimator().getJoinAfterFilterCardinality(context.getMetadataProvider(),
                        datasourcel.getDataset().getDataverseName(), datasourcel.getDataset().getDatasetName(),
                        leftFields, datasourcer.getDataset().getDataverseName(),
                        datasourcer.getDataset().getDatasetName(), rightFields, inputA.getCardinality());
            }
            if (inputB.getOperatorTag() != LogicalOperatorTag.SELECT) {
                inputB.setCardinality(context.getCardinalityEstimator().getTableCardinality(
                        context.getMetadataProvider(), datasourcer.getDataset().getDataverseName(),
                        datasourcer.getDataset().getDatasetName(), rightFields));
            } else {
                return context.getCardinalityEstimator().getJoinAfterFilterCardinality(context.getMetadataProvider(),
                        datasourcer.getDataset().getDataverseName(), datasourcer.getDataset().getDatasetName(),
                        rightFields, datasourcel.getDataset().getDataverseName(),
                        datasourcel.getDataset().getDatasetName(), leftFields, inputB.getCardinality());
            }

            return context.getCardinalityEstimator().getJoinCardinality(context.getMetadataProvider(),
                    datasourcel.getDataset().getDataverseName(), datasourcel.getDataset().getDatasetName(), leftFields,
                    datasourcer.getDataset().getDataverseName(), datasourcer.getDataset().getDatasetName(),
                    rightFields);
        }
        return CardinalityInferenceVisitor.UNKNOWN;
    }

    private long inferSelectCardinality(AbstractLogicalOperator op, IOptimizationContext context, DatasetDataSource ds,
            AssignOperator assign) throws AlgebricksException {
        context.addToDontApplySet(this, op);

        ILogicalExpression condExpr = null;
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            condExpr = ((SelectOperator) op).getCondition().getValue();
        }
        if (condExpr == null || condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return CardinalityInferenceVisitor.UNKNOWN;
        }

        IVariableTypeEnvironment typeEnvironment = context.getOutputTypeEnvironment(op);
        AccessMethodAnalysisContext analysisCtx = new AccessMethodAnalysisContext();
        boolean continueCheck = analyzeCondition(condExpr, context, typeEnvironment, analysisCtx);

        if (!continueCheck || analysisCtx.getMatchedFuncExprs().isEmpty()) {
            return CardinalityInferenceVisitor.UNKNOWN;
        }

        for (int j = 0; j < analysisCtx.getMatchedFuncExprs().size(); j++) {
            IOptimizableFuncExpr optFuncExpr = analysisCtx.getMatchedFuncExpr(j);
            fillOptimizableFuncExpr(optFuncExpr, ds, assign, false);
        }

        AIntegerObject lowKey = null;
        AIntegerObject highKey = null;
        int lowKeyAdjustment = 0;
        int highKeyAdjustment = 0;
        List<String> leftField = null;
        long cardinality = 1;
        long minCardinality = Long.MAX_VALUE;
        long tempCardinality = -1;
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.getMatchedFuncExprs()) {
            OptimizableOperatorSubTree optSubTree = optFuncExpr.getOperatorSubTree(0);
            LimitType limit = BTreeAccessMethod.getLimitType(optFuncExpr, optSubTree);
            //inferring cardinality for join
            if (optFuncExpr.getNumLogicalVars() == 1) {
                if (leftField == null) {
                    leftField = optFuncExpr.getFieldName(0);
                } else if (!leftField.equals(optFuncExpr.getFieldName(0))) {
                    // cannot calculate cardinality for expressions on different fields
                    return CardinalityInferenceVisitor.UNKNOWN;
                }
                ILogicalExpression expr = optFuncExpr.getConstantExpr(0);
                //inferring cardinality for selection
                switch (limit) {
                    case EQUAL:
                        highKey = extractConstantIntegerExpr(expr);
                        lowKey = highKey;
                        if (leftField != null) {
                            Long lowKeyValue = null;
                            Long highKeyValue = null;
                            lowKeyValue = lowKey.longValue() + lowKeyAdjustment;
                            highKeyValue = highKey.longValue() + highKeyAdjustment;
                            if (lowKeyValue != null && highKeyValue != null) {
                                tempCardinality = context.getCardinalityEstimator().getRangeCardinality(
                                        context.getMetadataProvider(), ds.getDataset().getDataverseName(),
                                        ds.getDataset().getDatasetName(), leftField, lowKeyValue, highKeyValue);
                                minCardinality = Math.min(minCardinality, tempCardinality);

                                cardinality *= tempCardinality;
                            }
                        }
                        leftField = null;
                        break;
                    case HIGH_EXCLUSIVE:
                        lowKeyAdjustment = 1;
                    case HIGH_INCLUSIVE:
                        lowKey = extractConstantIntegerExpr(expr);
                        break;
                    case LOW_EXCLUSIVE:
                        highKeyAdjustment = -1;
                    case LOW_INCLUSIVE:
                        highKey = extractConstantIntegerExpr(expr);
                        break;
                }
            }
        }
        op.setCardinality(minCardinality);
        return cardinality;
    }

    private AIntegerObject extractConstantIntegerExpr(ILogicalExpression expr) throws AsterixException {
        if (expr == null) {
            return null;
        }
        if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            IAObject constExprValue = ((AsterixConstantValue) ((ConstantExpression) expr).getValue()).getObject();
            if (ATypeHierarchy.belongsToDomain(constExprValue.getType().getTypeTag(), ATypeHierarchy.Domain.INTEGER)) {
                return (AIntegerObject) constExprValue;
            }
        }
        return null;

    }

    protected boolean analyzeCondition(ILogicalExpression cond, IOptimizationContext context,
            IVariableTypeEnvironment typeEnvironment, AccessMethodAnalysisContext analysisCtx)
            throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) cond;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (funcIdent == AlgebricksBuiltinFunctions.OR) {
            return false;
        } else if (funcIdent == AlgebricksBuiltinFunctions.AND) {
            analyzeFunctionExpr(funcExpr, analysisCtx, context, typeEnvironment);
            for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                ILogicalExpression argExpr = arg.getValue();
                if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    continue;
                }
                analyzeFunctionExpr((AbstractFunctionCallExpression) argExpr, analysisCtx, context, typeEnvironment);
            }
        } else {
            analyzeFunctionExpr(funcExpr, analysisCtx, context, typeEnvironment);
        }
        return true;
    }

    private void analyzeFunctionExpr(AbstractFunctionCallExpression funcExpr, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (funcIdent == AlgebricksBuiltinFunctions.EQ) {
            boolean matches = AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVarAndUpdateAnalysisCtx(funcExpr,
                    analysisCtx, context, typeEnvironment);
            if (!matches) {
                AccessMethodUtils.analyzeFuncExprArgsForTwoVarsAndUpdateAnalysisCtx(funcExpr, analysisCtx);
            }
        }
    }

    private boolean fillOptimizableFuncExpr(IOptimizableFuncExpr optFuncExpr, DatasetDataSource datasource,
            AssignOperator assign, boolean left) throws AlgebricksException {
        if (assign == null) {
            DataSourceScanOperator scan = null;
            if (left) {
                scan = (DataSourceScanOperator) lOp;
            } else {
                scan = (DataSourceScanOperator) sOp;
            }
            List<LogicalVariable> varList = scan.getVariables();
            for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                LogicalVariable var = varList.get(varIndex);
                int funcVarIndex = optFuncExpr.findLogicalVar(var);
                if (funcVarIndex == -1) {
                    continue;
                }
                int k = -1;
                List<String> fieldName = new ArrayList<>();
                for (LogicalVariable scanVar : scan.getVariables()) {
                    k++;
                    if (var == scanVar) {
                        fieldName.add(((InternalDatasetDetails) ((DatasetDataSource) scan.getDataSource()).getDataset()
                                .getDatasetDetails()).getPrimaryKey().get(k).get(0));
                    }
                }
                optFuncExpr.setFieldName(funcVarIndex, fieldName);
                return true;
            }
        }
        List<LogicalVariable> varList = assign.getVariables();
        for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
            LogicalVariable var = varList.get(varIndex);
            int funcVarIndex = optFuncExpr.findLogicalVar(var);
            if (funcVarIndex == -1) {
                continue;
            }
            AbstractLogicalExpression expr =
                    (AbstractLogicalExpression) assign.getExpressions().get(varIndex).getValue();
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            Integer idx = ConstantExpressionUtil.getIntArgument(funcExpr, 1);
            int fieldIndex = idx;
            ARecordType recordType = (ARecordType) datasource.getItemType();
            List<String> fieldName = new ArrayList<>();
            fieldName.add(recordType.getFieldNames()[fieldIndex]);
            optFuncExpr.setFieldName(funcVarIndex, fieldName);
            return true;
        }
        return false;
    }

}

class JoinNode {
    InnerJoinOperator join;
    AbstractLogicalOperator inputA;
    AbstractLogicalOperator inputB;

    public JoinNode(InnerJoinOperator join, AbstractLogicalOperator inputA, AbstractLogicalOperator inputB) {
        this.join = join;
        this.inputA = inputA;
        this.inputB = inputB;
    }
}