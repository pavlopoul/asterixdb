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
import static org.apache.asterix.optimizer.rules.am.OptimizableOperatorSubTree.DataSourceType.DATASOURCE_SCAN;

import java.util.List;
import java.util.Map;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.AIntegerObject;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.optimizer.rules.am.BTreeAccessMethod.LimitType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.CardinalityInferenceVisitor;

/**
 * Uses accumulated statistics to infer cardinality of the select/join operations
 * Matches the following patterns:
 * (select) <-- (assign)? <-- (datasource scan) or
 * (join) <-- (assign)? <--(datasource scan)
 * <-- (assign)? <-- (datasource scan)
 */
public class InferCardinalityRule extends AbstractIntroduceAccessMethodRule {

    private OptimizableOperatorSubTree leftSubTree;
    private OptimizableOperatorSubTree rightSubTree;

    protected boolean checkAndReturnExpr(AbstractLogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        // First check that the operator is a select or join and its condition is a function call.
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT || op.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
            leftSubTree = new OptimizableOperatorSubTree();
            leftSubTree.initFromSubTree(op.getInputs().get(0));
            if (!leftSubTree.setDatasetAndTypeMetadata(metadataProvider)) {
                return false;
            }
        } else {
            return false;
        }
        if (op.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
            rightSubTree = new OptimizableOperatorSubTree();
            if (!rightSubTree.initFromSubTree(op.getInputs().get(1))) {
                return false;
            }
            return rightSubTree.setDatasetAndTypeMetadata(metadataProvider);
        }
        return true;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (checkAndReturnExpr(op, context)) {
            op.setCardinality(inferCardinality(op, context));
            return true;
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getCardinality() == null && !op.getInputs().isEmpty()) {
            op.setCardinality(op.getInputs().get(0).getValue().getCardinality());
            return true;
        }
        return false;
    }

    private long inferCardinality(AbstractLogicalOperator op, IOptimizationContext context) throws AlgebricksException {
        context.addToDontApplySet(this, op);

        ILogicalExpression condExpr = null;
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            condExpr = ((SelectOperator) op).getCondition().getValue();
        } else if (op.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
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
            boolean matched = fillOptimizableFuncExpr(optFuncExpr, leftSubTree);
            if (rightSubTree != null) {
                matched &= fillOptimizableFuncExpr(optFuncExpr, rightSubTree);
            }
            if (!matched) {
                return CardinalityInferenceVisitor.UNKNOWN;
            }
        }

        AIntegerObject lowKey = null;
        AIntegerObject highKey = null;
        int lowKeyAdjustment = 0;
        int highKeyAdjustment = 0;
        List<String> leftField = null;
        List<String> rightField = null;
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.getMatchedFuncExprs()) {
            OptimizableOperatorSubTree optSubTree = optFuncExpr.getOperatorSubTree(0);
            LimitType limit = BTreeAccessMethod.getLimitType(optFuncExpr, optSubTree);
            //inferring cardinality for join
            if (optFuncExpr.getNumLogicalVars() == 2 && optFuncExpr.getOperatorSubTree(0) != null
                    && optFuncExpr.getOperatorSubTree(1) != null) {
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
                ILogicalExpression expr = optFuncExpr.getConstantExpr(0);
                //inferring cardinality for selection
                switch (limit) {
                    case EQUAL:
                        highKey = extractConstantIntegerExpr(expr);
                        lowKey = highKey;
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
        if (leftField != null && rightField != null) {
            //estimate join cardinality
            return context.getCardinalityEstimator().getJoinCardinality(context.getMetadataProvider(),
                    leftSubTree.getDataset().getDataverseName().getCanonicalForm(),
                    leftSubTree.getDataset().getDatasetName(), leftField,
                    rightSubTree.getDataset().getDataverseName().getCanonicalForm(),
                    rightSubTree.getDataset().getDatasetName(), rightField);
        } else if (leftField != null || rightField != null) {
            Long lowKeyValue = null;
            Long highKeyValue = null;
            if (lowKey != null) {
                lowKeyValue = lowKey.longValue() + lowKeyAdjustment;
            } else if (highKey != null) {
                lowKeyValue = highKey.minDomainValue();
            }
            if (highKey != null) {
                highKeyValue = highKey.longValue() + highKeyAdjustment;
            } else if (lowKey != null) {
                highKeyValue = lowKey.maxDomainValue();
            }
            if (lowKeyValue != null && highKeyValue != null) {
                return context.getCardinalityEstimator().getRangeCardinality(context.getMetadataProvider(),
                        leftSubTree.getDataset().getDataverseName().getCanonicalForm(),
                        leftSubTree.getDataset().getDatasetName(), leftField, lowKeyValue, highKeyValue);
            }
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
        if (funcIdent == AlgebricksBuiltinFunctions.LE || funcIdent == AlgebricksBuiltinFunctions.GE
                || funcIdent == AlgebricksBuiltinFunctions.LT || funcIdent == AlgebricksBuiltinFunctions.GT
                || funcIdent == AlgebricksBuiltinFunctions.EQ) {
            boolean matches = AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVarAndUpdateAnalysisCtx(funcExpr,
                    analysisCtx, context, typeEnvironment);
            if (!matches) {
                AccessMethodUtils.analyzeFuncExprArgsForTwoVarsAndUpdateAnalysisCtx(funcExpr, analysisCtx);
            }
        }
    }

    private AIntegerObject extractConstantIntegerExpr(ILogicalExpression expr) {
        if (expr == null) {
            return null;
        }
        if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            IAObject constExprValue = ((AsterixConstantValue) ((ConstantExpression) expr).getValue()).getObject();
            if (constExprValue.getType().getTypeTag() == ATypeTag.INTEGER) {
                return (AIntegerObject) constExprValue;
            }
        }
        return null;

    }

    private boolean fillOptimizableFuncExpr(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree subTree)
            throws AlgebricksException {
        if (subTree.getDataSourceType() == DATASOURCE_SCAN) {
            LogicalVariable datasetMetaVar = null;
            List<LogicalVariable> datasetVars = subTree.getDataSourceVariables();
            if (subTree.getDataset().hasMetaPart()) {
                datasetMetaVar = datasetVars.get(datasetVars.size() - 1);
            }
            MutableInt fieldSource = new MutableInt(0);
            for (int assignOrUnnestIndex = 0; assignOrUnnestIndex < subTree.getAssignsAndUnnests()
                    .size(); assignOrUnnestIndex++) {
                AbstractLogicalOperator subTreeOp = subTree.getAssignsAndUnnests().get(assignOrUnnestIndex);
                if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    List<LogicalVariable> varList = ((AssignOperator) subTreeOp).getVariables();
                    for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                        LogicalVariable var = varList.get(varIndex);
                        int funcVarIndex = optFuncExpr.findLogicalVar(var);
                        if (funcVarIndex == -1) {
                            continue;
                        }
                        List<String> fieldName = getFieldNameFromSubTree(null, subTree, assignOrUnnestIndex, varIndex,
                                subTree.getRecordType(), -1, null, subTree.getMetaRecordType(), datasetMetaVar,
                                fieldSource);
                        if (fieldName.isEmpty()) {
                            return false;
                        }
                        optFuncExpr.setFieldName(funcVarIndex, fieldName, fieldSource.intValue());
                        optFuncExpr.setOptimizableSubTree(funcVarIndex, subTree);
                        return true;
                    }
                }
            }
            // Try to match variables from optFuncExpr to datasourcescan if not
            // already matched in assigns.
            for (int varIndex = 0; varIndex < datasetVars.size(); varIndex++) {
                LogicalVariable var = datasetVars.get(varIndex);
                int funcVarIndex = optFuncExpr.findLogicalVar(var);
                // No matching var in optFuncExpr.
                if (funcVarIndex == -1) {
                    continue;
                }

                // The variable value is one of the partitioning fields.
                List<String> fieldName = null;
                List<List<String>> subTreePKs = null;
                subTreePKs = subTree.getDataset().getPrimaryKeys();
                // Check whether this variable is PK, not a record variable.
                if (varIndex <= subTreePKs.size() - 1) {
                    fieldName = subTreePKs.get(varIndex);

                }
                optFuncExpr.setFieldName(funcVarIndex, fieldName, fieldSource.intValue());
                optFuncExpr.setOptimizableSubTree(funcVarIndex, subTree);
                optFuncExpr.setSourceVar(funcVarIndex, var);
                optFuncExpr.setLogicalExpr(funcVarIndex, new VariableReferenceExpression(var));
                return true;
            }
        }
        return false;
    }

    @Override
    public Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods() {
        // TODO Auto-generated method stub
        return null;
    }
}
