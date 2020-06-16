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

import static org.apache.asterix.optimizer.rules.am.BTreeAccessMethod.LimitType.EQUAL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ARecordType;
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

    private Mutable<ILogicalOperator> mut;
    private Boolean b = false;
    private TreeMap<Long, List<Mutable<ILogicalOperator>>> map = new TreeMap<>(Collections.reverseOrder());
    private AbstractLogicalOperator alo, sOp, lOp;
    private String fieldName = "";
    private String[] pKey;
    private LogicalVariable[] dscanvar;
    private SelectOperator select;

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
                    b = true;
                    // populateMap(op.getInputs(), datasourcer, assignr, context, only2joins);
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
                if (sfce.getFunctionIdentifier().getName() == "and") {
                    long key = Long.MAX_VALUE;
                    for (Mutable<ILogicalExpression> arg : sfce.getArguments()) {
                        join = new InnerJoinOperator(arg, join.getInputs().get(0), join.getInputs().get(1));
                        context.computeAndSetTypeEnvironmentForOperator(join);
                        sfce = (ScalarFunctionCallExpression) arg.getValue();
                        LogicalVariable lvl = ((VariableReferenceExpression) sfce.getArguments().get(0).getValue())
                                .getVariableReference();
                        DatasetDataSource datasourcel = findDataSource(join, lvl);
                        AssignOperator assignl = null;
                        lOp = sOp;
                        if (sOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                            assignl = (AssignOperator) sOp;
                        }
                        LogicalVariable lvr = ((VariableReferenceExpression) sfce.getArguments().get(1).getValue())
                                .getVariableReference();
                        datasourcer = findDataSource(join, lvr);
                        if (sOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                            assignr = (AssignOperator) sOp;
                        } else {
                            assignr = null;
                        }
                        if (only2joins) {
                            key = Math.min(key, inferCardinality((AbstractLogicalOperator) /*child.getValue()*/join,
                                    context, datasourcel, datasourcer, assignl, assignr));
                            System.out.println(key);
                        } else {
                            key = inferCardinality((AbstractLogicalOperator) /*child.getValue()*/join, context,
                                    datasourcel, datasourcer, assignl, assignr);
                            System.out.println(key);
                        }
                        if (!only2joins) {
                            if (map.containsKey(key)) {
                                map.get(key).add(new MutableObject<ILogicalOperator>(join));
                            } else {
                                map.put(key, new ArrayList<>());
                                map.get(key).add(new MutableObject<ILogicalOperator>(join));
                            }
                        }
                    }
                    if (only2joins) {
                        if (map.containsKey(key)) {
                            map.get(key).add(child);
                        } else {
                            map.put(key, new ArrayList<>());
                            map.get(key).add(child);
                        }
                    }

                } else {
                    LogicalVariable lvl = ((VariableReferenceExpression) sfce.getArguments().get(0).getValue())
                            .getVariableReference();
                    DatasetDataSource datasourcel = findDataSource(join, lvl);
                    AssignOperator assignl = null;
                    lOp = sOp;
                    if (sOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                        assignl = (AssignOperator) sOp;
                    }
                    LogicalVariable lvr = ((VariableReferenceExpression) sfce.getArguments().get(1).getValue())
                            .getVariableReference();
                    datasourcer = findDataSource(join, lvr);
                    if (sOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                        assignr = (AssignOperator) sOp;
                    } else {
                        assignr = null;
                    }
                    long key = inferCardinality((AbstractLogicalOperator) child.getValue(), context, datasourcel,
                            datasourcer, assignl, assignr);
                    System.out.println(key);
                    if (map.containsKey(key)) {
                        map.get(key).add(child);
                    } else {
                        map.put(key, new ArrayList<>());
                        map.get(key).add(child);
                    }
                }
                populateMap(child.getValue().getInputs(), datasourcer, assignr, context, only2joins);
            }
        }
    }

    public DatasetDataSource findDataSource(AbstractLogicalOperator op, LogicalVariable lv) {
        DatasetDataSource datasource = null;
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN
                && op.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            for (Mutable<ILogicalOperator> child : op.getInputs()) {
                datasource = findDataSource((AbstractLogicalOperator) child.getValue(), lv);
                if (datasource != null) {
                    if (child.getValue().getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                        mut = child;
                    } else if (child.getValue().getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                        mut = child;
                    }
                    break;
                }
            }
            return datasource;
        }
        AssignOperator assign = null;
        DataSourceScanOperator scan = null;
        boolean vfromAssign = false;
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            assign = (AssignOperator) op;
            scan = /*null*/(DataSourceScanOperator) assign.getInputs().get(0).getValue();
            int i = -1;
            for (LogicalVariable assignVar : assign.getVariables()) {
                i++;
                if (lv == assignVar) {
                    vfromAssign = true;
                    ScalarFunctionCallExpression sfce =
                            (ScalarFunctionCallExpression) assign.getExpressions().get(i).getValue();
                    ConstantExpression ce = (ConstantExpression) sfce.getArguments().get(1).getValue();
                    AsterixConstantValue acv = (AsterixConstantValue) ce.getValue();
                    AInt32 aint32 = (AInt32) acv.getObject();
                    int field = aint32.getIntegerValue();
                    fieldName = ((ARecordType) ((DatasetDataSource) scan.getDataSource()).getItemType())
                            .getFieldNames()[field];
                    int size = scan.getVariables().size() - 1;
                    pKey = new String[size];
                    dscanvar = new LogicalVariable[size];
                    for (int j = 0; j < size; j++) {
                        pKey[j] = ((ARecordType) ((DatasetDataSource) scan.getDataSource()).getItemType())
                                .getFieldNames()[j];
                        dscanvar[j] = scan.getVariables().get(j);
                    }

                    sOp = assign;
                }
            }
        } else {
            scan = (DataSourceScanOperator) op;
        }
        if (!vfromAssign) {
            for (LogicalVariable scanVar : scan.getVariables()) {
                if (lv == scanVar) {
                    vfromAssign = true;
                    fieldName =
                            ((ARecordType) ((DatasetDataSource) scan.getDataSource()).getItemType()).getFieldNames()[0];
                    int size = scan.getVariables().size() - 1;
                    pKey = new String[size];
                    dscanvar = new LogicalVariable[size];
                    for (int j = 0; j < size; j++) {
                        pKey[j] = ((ARecordType) ((DatasetDataSource) scan.getDataSource()).getItemType())
                                .getFieldNames()[j];
                        dscanvar[j] = scan.getVariables().get(j);
                    }
                    sOp = scan;
                }
            }
        }
        if (!vfromAssign) {
            scan = null;
        }
        return scan != null ? (DatasetDataSource) scan.getDataSource() : datasource;
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
        if (!b)
            return false;
        InnerJoinOperator joinA = (InnerJoinOperator) alo.getInputs().get(0).getValue();
        InnerJoinOperator joinB = (InnerJoinOperator) joinA.getInputs().get(0).getValue();
        InnerJoinOperator joinC = (InnerJoinOperator) joinB.getInputs().get(0).getValue();

        Mutable<ILogicalExpression> condition = joinA.getCondition();
        ScalarFunctionCallExpression sfce = (ScalarFunctionCallExpression) condition.getValue();
        List<Mutable<ILogicalExpression>> arguments = sfce.getArguments();
        LogicalVariable lvl = ((VariableReferenceExpression) arguments.get(0).getValue()).getVariableReference();
        DatasetDataSource ds = findDataSource((AbstractLogicalOperator) joinA, lvl);

        joinA.getInputs().set(0, mut);
        ScalarFunctionCallExpression newsfce =
                ((ScalarFunctionCallExpression) joinB.getCondition().getValue()).cloneExpression();
        List<Mutable<ILogicalExpression>> narguments = newsfce.getArguments();
        Mutable<ILogicalExpression> mule = narguments.get(0);
        newsfce.getArguments().set(0, narguments.get(1));

        newsfce.getArguments().set(1, mule);
        Mutable<ILogicalExpression> conditionB = joinC.getCondition();
        ScalarFunctionCallExpression sfceB = (ScalarFunctionCallExpression) conditionB.getValue();
        List<Mutable<ILogicalExpression>> argumentsB = sfceB.getArguments();
        newsfce.getArguments().set(1, argumentsB.get(0));
        InnerJoinOperator finalJoin =
                new InnerJoinOperator(joinB.getCondition(), new MutableObject<>(joinC), new MutableObject<>(joinA));
        context.computeAndSetTypeEnvironmentForOperator(finalJoin);
        alo.getInputs().clear();
        alo.getInputs().add(new MutableObject<>(finalJoin));
        b = false;
        return true;

    }

    public LogicalVariable findSelect(ILogicalOperator op) {
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            select = (SelectOperator) op;
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
            DatasetDataSource datasourcel, DatasetDataSource datasourcer, AssignOperator assignl,
            AssignOperator assignr) throws AlgebricksException {
        context.addToDontApplySet(this, op);

        ILogicalExpression condExpr = null;
        if (op.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
            condExpr = ((InnerJoinOperator) op).getCondition().getValue();
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
            fillOptimizableFuncExpr(optFuncExpr, datasourcel, assignl, true);
            fillOptimizableFuncExpr(optFuncExpr, datasourcer, assignr, false);
        }

        List<String> leftField = null;
        List<String> rightField = null;
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.getMatchedFuncExprs()) {
            OptimizableOperatorSubTree optSubTree = optFuncExpr.getOperatorSubTree(0);
            LimitType limit = BTreeAccessMethod.getLimitType(optFuncExpr, optSubTree);
            //inferring cardinality for join
            if (optFuncExpr.getNumLogicalVars() == 2) {
                // cannot calculate cardinality for non equi-joins
                if (limit != EQUAL) {
                    return CardinalityInferenceVisitor.UNKNOWN;
                }
                // cannot calculate cardinality for complex (conjunctive) join conditions
                if (leftField != null && rightField != null) {
                    return CardinalityInferenceVisitor.UNKNOWN;
                }
                leftField = optFuncExpr.getFieldName(0);
                rightField = optFuncExpr.getFieldName(1);
            } else if (optFuncExpr.getNumLogicalVars() == 1) {
                if (leftField == null) {
                    leftField = optFuncExpr.getFieldName(0);
                } else if (!leftField.equals(optFuncExpr.getFieldName(0))) {
                    // cannot calculate cardinality for expressions on different fields
                    return CardinalityInferenceVisitor.UNKNOWN;
                }
            }
        }
        if (leftField != null && rightField != null) {
            System.out.println(
                    datasourcel.getDataset().getDatasetName() + " " + datasourcer.getDataset().getDatasetName());
            //estimate join cardinality
            return context.getCardinalityEstimator().getJoinCardinality(context.getMetadataProvider(),
                    datasourcel.getDataset().getDataverseName(), datasourcel.getDataset().getDatasetName(), leftField,
                    datasourcer.getDataset().getDataverseName(), datasourcer.getDataset().getDatasetName(), rightField);
        }
        return CardinalityInferenceVisitor.UNKNOWN;
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

                ARecordType recordType = (ARecordType) datasource.getItemType();
                List<String> fieldName = new ArrayList<>();
                fieldName.add(recordType.getFieldNames()[0]);
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
