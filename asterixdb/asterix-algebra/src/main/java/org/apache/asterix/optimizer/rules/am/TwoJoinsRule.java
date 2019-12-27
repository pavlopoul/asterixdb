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

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DatasetDataSource;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.CardinalityInferenceVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class TwoJoinsRule implements IAlgebraicRewriteRule {

    private AssignOperator subOp, assignrf;
    private DatasetDataSource datasourcerf;
    private Mutable<ILogicalOperator> mut;
    private TreeMap<Long, List<Mutable<ILogicalOperator>>> map = new TreeMap<>(Collections.reverseOrder());
    private AbstractLogicalOperator alo;
    private boolean first = true;

    // private boolean matched = false;

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
                Mutable<ILogicalOperator> mutalele = op.getInputs().get(0);
                if (childop.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                    if (childop.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.INNERJOIN
                            && childop.getInputs().get(1).getValue().getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
                        return false;
                    }
                    AbstractLogicalOperator right = (AbstractLogicalOperator) childop.getInputs().get(1).getValue();
                    InnerJoinOperator joinout = (InnerJoinOperator) childop;
                    Mutable<ILogicalExpression> conditionout = joinout.getCondition();
                    ScalarFunctionCallExpression sfceout = (ScalarFunctionCallExpression) conditionout.getValue();
                    LogicalVariable lvlout = ((VariableReferenceExpression) sfceout.getArguments().get(1).getValue())
                            .getVariableReference();
                    DatasetDataSource datasource = findDataSource(right, lvlout);
                    AssignOperator assignout = subOp;
                    DatasetDataSource datasourcer = null;
                    AssignOperator assignr = null;
                    populateMap(childop.getInputs(), datasourcer, assignr, context);
                    long key = inferCardinality(joinout, context, datasourcerf, datasource, assignrf, assignout);
                    if (map.containsKey(key)) {
                        map.get(key).add(mutalele);
                    } else {
                        map.put(key, new ArrayList<>());
                        map.get(key).add(mutalele);
                    }
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
            AssignOperator assignr, IOptimizationContext context) throws AlgebricksException {
        for (Mutable<ILogicalOperator> child : inputs) {
            if (child.getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                InnerJoinOperator join = (InnerJoinOperator) child.getValue();
                Mutable<ILogicalExpression> condition = join.getCondition();
                ScalarFunctionCallExpression sfce = (ScalarFunctionCallExpression) condition.getValue();
                LogicalVariable lvl =
                        ((VariableReferenceExpression) sfce.getArguments().get(0).getValue()).getVariableReference();
                DatasetDataSource datasourcel = findDataSource(join, lvl);
                AssignOperator assignl = subOp;
                LogicalVariable lvr =
                        ((VariableReferenceExpression) sfce.getArguments().get(1).getValue()).getVariableReference();
                if (first) {
                    datasourcerf = findDataSource(join, lvr);
                    assignrf = subOp;
                    first = false;
                }
                datasourcer = findDataSource(join, lvr);
                assignr = subOp;
                long key = inferCardinality((AbstractLogicalOperator) child.getValue(), context, datasourcel,
                        datasourcer, assignl, assignr);
                if (map.containsKey(key)) {
                    map.get(key).add(child);
                } else {
                    map.put(key, new ArrayList<>());
                    map.get(key).add(child);
                }
                populateMap(child.getValue().getInputs(), datasourcer, assignr, context);
            }
        }
    }

    public DatasetDataSource findDataSource(AbstractLogicalOperator op, LogicalVariable lv) {
        DatasetDataSource datasource = null;
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            for (Mutable<ILogicalOperator> child : op.getInputs()) {
                datasource = findDataSource((AbstractLogicalOperator) child.getValue(), lv);
                if (datasource != null) {
                    if (child.getValue().getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                        mut = child;
                    }
                    break;
                }
            }
            return datasource;
        }
        AssignOperator assign = (AssignOperator) op;
        DataSourceScanOperator scan = null;
        for (LogicalVariable assignVar : assign.getVariables()) {
            if (lv == assignVar) {
                scan = (DataSourceScanOperator) assign.getInputs().get(0).getValue();
                subOp = assign;
            }
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

        if (alo != null && !map.isEmpty()) {
            TreeMap<Long, List<Mutable<ILogicalOperator>>> twomap = new TreeMap<>();
            twomap.put(map.lastKey(), map.lastEntry().getValue());
            map.remove(map.lastKey());
            twomap.put(map.lastKey(), map.lastEntry().getValue());
            InnerJoinOperator joinA = (InnerJoinOperator) twomap.firstEntry().getValue().get(0).getValue();

            Mutable<ILogicalExpression> conditionA = joinA.getCondition();
            ScalarFunctionCallExpression sfceA = (ScalarFunctionCallExpression) conditionA.getValue();
            LogicalVariable lvrA =
                    ((VariableReferenceExpression) sfceA.getArguments().get(1).getValue()).getVariableReference();
            LogicalVariable lvlA =
                    ((VariableReferenceExpression) sfceA.getArguments().get(0).getValue()).getVariableReference();
            int inputs = -1;
            for (Mutable<ILogicalOperator> mlo : joinA.getInputs()) {
                inputs++;
                if (mlo.getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                    LogicalVariable lv = null;
                    for (Mutable<ILogicalExpression> mule : ((ScalarFunctionCallExpression) ((InnerJoinOperator) mlo
                            .getValue()).getCondition().getValue()).getArguments()) {
                        if (((VariableReferenceExpression) mule.getValue()).getVariableReference() == lvrA) {
                            lv = lvrA;
                            break;
                        } else if (((VariableReferenceExpression) mule.getValue()).getVariableReference() == lvlA) {
                            lv = lvlA;
                            break;
                        }
                    }
                    findDataSource((AbstractLogicalOperator) mlo.getValue(), lv);
                    joinA.getInputs().set(inputs, mut);
                }
            }
            InnerJoinOperator joinB = (InnerJoinOperator) twomap.lastEntry().getValue().get(0).getValue();
            Mutable<ILogicalExpression> conditionB = joinB.getCondition();
            ScalarFunctionCallExpression sfceB = (ScalarFunctionCallExpression) conditionB.getValue();
            LogicalVariable lvlB =
                    ((VariableReferenceExpression) sfceB.getArguments().get(0).getValue()).getVariableReference();
            LogicalVariable lvrB =
                    ((VariableReferenceExpression) sfceB.getArguments().get(1).getValue()).getVariableReference();
            inputs = -1;
            for (Mutable<ILogicalOperator> mlo : joinB.getInputs()) {
                inputs++;
                if (mlo.getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                    LogicalVariable lv = null;
                    for (Mutable<ILogicalExpression> mule : ((ScalarFunctionCallExpression) ((InnerJoinOperator) mlo
                            .getValue()).getCondition().getValue()).getArguments()) {
                        if (((VariableReferenceExpression) mule.getValue()).getVariableReference() == lvrB) {
                            lv = lvrB;
                            break;
                        } else if (((VariableReferenceExpression) mule.getValue()).getVariableReference() == lvlB) {
                            lv = lvlB;
                            break;
                        }
                    }
                    findDataSource((AbstractLogicalOperator) mlo.getValue(), lv);
                    joinB.getInputs().set(inputs, mut);
                }
            }
            if (lvlA == lvlB || lvlA == lvrB) {
                joinA.getInputs().set(0, new MutableObject<ILogicalOperator>(joinB));
                alo.getInputs().clear();
                alo.getInputs().add(new MutableObject<ILogicalOperator>(joinA));
            } else if (lvrA == lvrB || lvrA == lvlB) {
                joinA.getInputs().set(1, new MutableObject<ILogicalOperator>(joinB));
                alo.getInputs().clear();
                alo.getInputs().add(new MutableObject<ILogicalOperator>(joinA));
            } else {
                List<Mutable<ILogicalExpression>> eqExprs = new ArrayList<Mutable<ILogicalExpression>>();
                List<Mutable<ILogicalExpression>> args = new ArrayList<Mutable<ILogicalExpression>>();
                args.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(lvrA)));
                args.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(lvlB)));
                ScalarFunctionCallExpression eqFunc = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.EQ), args);
                eqExprs.add(new MutableObject<ILogicalExpression>(eqFunc));
                InnerJoinOperator finalJoin = new InnerJoinOperator(eqExprs.get(0),
                        twomap.firstEntry().getValue().get(0), twomap.lastEntry().getValue().get(0));
                context.computeAndSetTypeEnvironmentForOperator(finalJoin);
                alo.getInputs().clear();
                alo.getInputs().add(new MutableObject<ILogicalOperator>(finalJoin));
            }
            LogicalVariable lv =
                    ((VariableReferenceExpression) ((AssignOperator) alo).getExpressions().get(0).getValue())
                            .getVariableReference();
            if (findDataSource(alo, lv) == null) {
                ((AssignOperator) alo).getExpressions().set(0, sfceB.getArguments().get(0));
            }
            map.clear();
            return true;
        }
        return false;
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
            fillOptimizableFuncExpr(optFuncExpr, datasourcel, assignl);
            fillOptimizableFuncExpr(optFuncExpr, datasourcer, assignr);
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
            AssignOperator assign) throws AlgebricksException {
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
