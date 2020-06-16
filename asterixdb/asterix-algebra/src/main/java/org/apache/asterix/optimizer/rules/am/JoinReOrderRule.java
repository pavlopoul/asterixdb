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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AIntegerObject;
import org.apache.asterix.om.base.AString;
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

public class JoinReOrderRule implements IAlgebraicRewriteRule {

    private Mutable<ILogicalOperator> mut;
    private TreeMap<Long, List<Mutable<ILogicalOperator>>> map = new TreeMap<>(Collections.reverseOrder());
    private List<Mutable<ILogicalOperator>> selectSet = new ArrayList<>();
    private AbstractLogicalOperator alo, sOp, lOp;
    private String fieldName = "";
    private Map<DatasetDataSource, List<Node>> fields = new HashMap<>();

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
                                    context, datasourcel, datasourcer, assignl, assignr, lvl, lvr));
                            System.out.println(key);
                        } else {
                            key = inferCardinality((AbstractLogicalOperator) /*child.getValue()*/join, context,
                                    datasourcel, datasourcer, assignl, assignr, lvl, lvr);
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
                    String leftField = fieldName;
                    AssignOperator assignl = null;
                    lOp = sOp;
                    if (sOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                        assignl = (AssignOperator) sOp;
                    }
                    LogicalVariable lvr = ((VariableReferenceExpression) sfce.getArguments().get(1).getValue())
                            .getVariableReference();
                    datasourcer = findDataSource(join, lvr);
                    if (join.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.SELECT
                            || join.getInputs().get(1).getValue().getOperatorTag() == LogicalOperatorTag.SELECT) {
                        DatasetDataSource selectds;
                        AbstractLogicalOperator op;
                        if (join.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.SELECT) {
                            selectds = datasourcel;
                            op = (AbstractLogicalOperator) join.getInputs().get(0).getValue();
                        } else {
                            selectds = datasourcer;
                            op = (AbstractLogicalOperator) join.getInputs().get(1).getValue();
                        }
                        selectSet.add(new MutableObject<>(join));
                        if (fields.containsKey(datasourcel)) {
                            populateFields(false, datasourcel, leftField, lvl);
                        }
                        if (fields.containsKey(datasourcer)) {
                            populateFields(false, datasourcer, fieldName, lvr);
                        }
                        if (!fields.containsKey(datasourcel)) {
                            populateFields(true, datasourcel, leftField, lvl);
                        }
                        if (!fields.containsKey(datasourcer)) {
                            populateFields(true, datasourcer, fieldName, lvr);
                        }
                        inferSelectCardinality(op, context, selectds,
                                (AssignOperator) op.getInputs().get(0).getValue());

                    } else {
                        if (sOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                            assignr = (AssignOperator) sOp;
                        } else {
                            assignr = null;
                        }
                        long key = inferCardinality((AbstractLogicalOperator) child.getValue(), context, datasourcel,
                                datasourcer, assignl, assignr, lvl, lvr);
                        child.getValue().setCardinality(key);
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
            scan = (DataSourceScanOperator) assign.getInputs().get(0).getValue();
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
                    sOp = assign;
                }
            }
        } else {
            scan = (DataSourceScanOperator) op;
        }
        if (!vfromAssign) {
            int k = -1;
            for (LogicalVariable scanVar : scan.getVariables()) {
                k++;
                if (lv == scanVar) {
                    vfromAssign = true;
                    fieldName = ((InternalDatasetDetails) ((DatasetDataSource) scan.getDataSource()).getDataset()
                            .getDatasetDetails()).getPrimaryKey().get(k).get(0);
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

    private void checkForJoin(InnerJoinOperator join, LogicalVariable left, LogicalVariable right) {
        for (Mutable<ILogicalOperator> mlo : join.getInputs()) {
            int inputs = 0;
            if (mlo.getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                DatasetDataSource ds = findDataSource((AbstractLogicalOperator) mlo.getValue(), left);

                if (ds != null) {
                    join.getInputs().set(inputs, mut);

                }
                inputs++;
                DatasetDataSource dsr = findDataSource((AbstractLogicalOperator) mlo.getValue(), right);
                if (dsr != null) {
                    join.getInputs().set(inputs, mut);

                }
            }
        }
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (alo != null && !map.isEmpty()) {
            InnerJoinOperator joinA;
            LogicalVariable lvrA;
            LogicalVariable lvlA;
            if (!selectSet.isEmpty()) {
                joinA = (InnerJoinOperator) selectSet.get(0).getValue();
                Mutable<ILogicalExpression> conditionA = joinA.getCondition();

                ScalarFunctionCallExpression sfceA = (ScalarFunctionCallExpression) conditionA.getValue();

                lvrA = ((VariableReferenceExpression) sfceA.getArguments().get(1).getValue()).getVariableReference();
                lvlA = ((VariableReferenceExpression) sfceA.getArguments().get(0).getValue()).getVariableReference();

                // checkForJoin(joinA, lvlA, lvrA);
            } else {
                TreeMap<Long, List<Mutable<ILogicalOperator>>> twomap = new TreeMap<>();
                twomap.put(map.lastKey(), map.lastEntry().getValue());
                int size = map.size();

                map.remove(map.lastKey());
                if (size > 1) {
                    twomap.put(map.lastKey(), map.lastEntry().getValue());
                }
                joinA = (InnerJoinOperator) twomap.firstEntry().getValue().get(0).getValue();

                Mutable<ILogicalExpression> conditionA = joinA.getCondition();

                ScalarFunctionCallExpression sfceA = (ScalarFunctionCallExpression) conditionA.getValue();
                if (sfceA.getFunctionIdentifier().getName() == "and") {
                    map.clear();
                    return false;
                }
                lvrA = ((VariableReferenceExpression) sfceA.getArguments().get(1).getValue()).getVariableReference();
                lvlA = ((VariableReferenceExpression) sfceA.getArguments().get(0).getValue()).getVariableReference();
                checkForJoin(joinA, lvlA, lvrA);
                context.computeAndSetTypeEnvironmentForOperator(joinA);
                if ((size == 2 && twomap.firstEntry().getValue().size() == 1
                        && twomap.lastEntry().getValue().size() == 1) || size == 1) {
                    construct2Joins(size, twomap, context, joinA, lvlA, lvrA);
                    map.clear();
                    return true;
                }
            }
            return constructAssign(joinA, lvlA, lvrA);
        }
        return false;

    }

    private void construct2Joins(int size, TreeMap<Long, List<Mutable<ILogicalOperator>>> twomap,
            IOptimizationContext context, InnerJoinOperator joinA, LogicalVariable lvlA, LogicalVariable lvrA)
            throws AlgebricksException {
        InnerJoinOperator joinB;
        if (size == 1) {
            joinB = (InnerJoinOperator) twomap.lastEntry().getValue().get(1).getValue();

        } else {
            joinB = (InnerJoinOperator) twomap.lastEntry().getValue().get(0).getValue();
        }
        Mutable<ILogicalExpression> conditionB = joinB.getCondition();

        ScalarFunctionCallExpression sfceB = (ScalarFunctionCallExpression) conditionB.getValue();
        LogicalVariable lvlB;
        LogicalVariable lvrB;
        if (joinB.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && joinB.getInputs().get(1).getValue().getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            if (sfceB.getFunctionIdentifier().getName() == "and") {
                ScalarFunctionCallExpression sfc =
                        (ScalarFunctionCallExpression) sfceB.getArguments().get(0).getValue();
                lvlB = ((VariableReferenceExpression) sfc.getArguments().get(0).getValue()).getVariableReference();
                lvrB = ((VariableReferenceExpression) sfc.getArguments().get(1).getValue()).getVariableReference();
            } else {
                lvlB = ((VariableReferenceExpression) sfceB.getArguments().get(0).getValue()).getVariableReference();
                lvrB = ((VariableReferenceExpression) sfceB.getArguments().get(1).getValue()).getVariableReference();
            }
            DatasetDataSource datasourcelB = findDataSource(joinB, lvlB);
            DatasetDataSource datasourcerB = findDataSource(joinB, lvrB);
            DatasetDataSource datasourcelA = findDataSource(joinA, lvlA);
            DatasetDataSource datasourcerA = findDataSource(joinA, lvrA);

            if (datasourcelB == datasourcelA || datasourcelB == datasourcerA) {
                InnerJoinOperator finaljoin = new InnerJoinOperator(joinB.getCondition(),
                        new MutableObject<ILogicalOperator>(joinA), joinB.getInputs().get(1));
                context.computeAndSetTypeEnvironmentForOperator(finaljoin);
                alo.getInputs().clear();
                alo.getInputs().add(new MutableObject<ILogicalOperator>(finaljoin));
            } else if (datasourcerB == datasourcerA || datasourcerB == datasourcelA) {
                InnerJoinOperator finaljoin = new InnerJoinOperator(joinB.getCondition(), joinB.getInputs().get(0),
                        new MutableObject<ILogicalOperator>(joinA));
                context.computeAndSetTypeEnvironmentForOperator(finaljoin);
                alo.getInputs().clear();
                alo.getInputs().add(new MutableObject<ILogicalOperator>(finaljoin));
            }
        }
    }

    private Boolean constructAssign(InnerJoinOperator join, LogicalVariable lvl, LogicalVariable lvr) {
        alo.getInputs().clear();

        if (join.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.SELECT
                || join.getInputs().get(1).getValue().getOperatorTag() == LogicalOperatorTag.SELECT) {
            SelectOperator select;
            if (join.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.SELECT) {
                select = (SelectOperator) join.getInputs().get(0).getValue();
            } else {
                select = (SelectOperator) join.getInputs().get(1).getValue();
            }
            ScalarFunctionCallExpression sfce = (ScalarFunctionCallExpression) select.getCondition().getValue();
            LogicalVariable selectLv = null;
            if (sfce.getArguments().get(0).getValue() instanceof ScalarFunctionCallExpression) {
                ScalarFunctionCallExpression innerSfce =
                        (ScalarFunctionCallExpression) sfce.getArguments().get(0).getValue();
                selectLv = ((VariableReferenceExpression) innerSfce.getArguments().get(0).getValue())
                        .getVariableReference();
            } else {
                selectLv = ((VariableReferenceExpression) sfce.getArguments().get(0).getValue()).getVariableReference();
            }
            alo.getInputs().add(new MutableObject<ILogicalOperator>(select));
            DatasetDataSource ds = findDataSource(select, selectLv);
            List<Mutable<ILogicalExpression>> arguments =
                    ((ScalarFunctionCallExpression) ((AssignOperator) alo).getExpressions().get(0).getValue())
                            .getArguments();
            arguments.clear();
            addArguments(ds, arguments);
        } else {
            alo.getInputs().add(new MutableObject<ILogicalOperator>(join));
            LogicalVariable lv = ((VariableReferenceExpression) ((ScalarFunctionCallExpression) ((AssignOperator) alo)
                    .getExpressions().get(0).getValue()).getArguments().get(1).getValue()).getVariableReference();
            DatasetDataSource dsl = findDataSource(join, lvl);
            removeParticipation(dsl);

            DatasetDataSource dsr = findDataSource(join, lvr);
            removeParticipation(dsr);
            List<Mutable<ILogicalExpression>> arguments =
                    ((ScalarFunctionCallExpression) ((AssignOperator) alo).getExpressions().get(0).getValue())
                            .getArguments();
            if (findDataSource(join, lv) == null) {
                arguments.clear();
            }

            addArguments(dsl, arguments);
            addArguments(dsr, arguments);
        }
        map.clear();
        return true;
    }

    private void removeParticipation(DatasetDataSource ds) {
        for (Node n : fields.get(ds)) {
            if (n.field.equals(fieldName)) {
                n.participation--;
                break;
            }
        }
    }

    private void addArguments(DatasetDataSource ds, List<Mutable<ILogicalExpression>> arguments) {
        for (Node n : fields.get(ds)) {
            if (n.participation > 0) {
                IAObject obj = new AString(n.field);
                AsterixConstantValue acv = new AsterixConstantValue(obj);
                ConstantExpression ce = new ConstantExpression(acv);
                arguments.add(new MutableObject<>(ce));
                arguments.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(n.lv)));
            }
        }
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
            DatasetDataSource datasourcel, DatasetDataSource datasourcer, AssignOperator assignl,
            AssignOperator assignr, LogicalVariable left, LogicalVariable right) throws AlgebricksException {
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
            if (fields.containsKey(datasourcel)) {
                populateFields(false, datasourcel, leftField.get(0), left);
            }
            if (fields.containsKey(datasourcer)) {
                populateFields(false, datasourcer, rightField.get(0), right);
            }
            if (!fields.containsKey(datasourcel)) {
                populateFields(true, datasourcel, leftField.get(0), left);
            }
            if (!fields.containsKey(datasourcer)) {
                populateFields(true, datasourcer, rightField.get(0), right);
            }
            if (assignl != null) {
                assignl.setCardinality(context.getCardinalityEstimator().getTableCardinality(
                        context.getMetadataProvider(), datasourcel.getDataset().getDataverseName(),
                        datasourcel.getDataset().getDatasetName(), leftField));
            } else {
                op.getInputs().get(0).getValue()
                        .setCardinality(context.getCardinalityEstimator().getTableCardinality(
                                context.getMetadataProvider(), datasourcel.getDataset().getDataverseName(),
                                datasourcel.getDataset().getDatasetName(), leftField));
            }
            if (assignr != null) {
                assignr.setCardinality(context.getCardinalityEstimator().getTableCardinality(
                        context.getMetadataProvider(), datasourcer.getDataset().getDataverseName(),
                        datasourcer.getDataset().getDatasetName(), rightField));
            } else {
                op.getInputs().get(1).getValue()
                        .setCardinality(context.getCardinalityEstimator().getTableCardinality(
                                context.getMetadataProvider(), datasourcer.getDataset().getDataverseName(),
                                datasourcer.getDataset().getDatasetName(), rightField));
            }
            System.out.println(
                    datasourcel.getDataset().getDatasetName() + " " + datasourcer.getDataset().getDatasetName());
            //estimate join cardinality
            return context.getCardinalityEstimator().getJoinCardinality(context.getMetadataProvider(),
                    datasourcel.getDataset().getDataverseName(), datasourcel.getDataset().getDatasetName(), leftField,
                    datasourcer.getDataset().getDataverseName(), datasourcer.getDataset().getDatasetName(), rightField);
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
                        if (highKey == null || lowKey == null) {
                            return CardinalityInferenceVisitor.UNKNOWN;
                        }
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

    private void populateFields(boolean notInFields, DatasetDataSource ds, String field, LogicalVariable lv) {
        if (notInFields) {
            fields.put(ds, new ArrayList<>());
            Node n = new Node(field, 1, lv);
            fields.get(ds).add(n);
        } else {
            Boolean found = false;
            for (Node n : fields.get(ds)) {
                if (n.field.equals(field)) {
                    found = true;
                    n.participation++;
                    break;
                }
            }
            if (!found) {
                fields.get(ds).add(new Node(field, 1, lv));
            }
        }

    }

}

class Node {
    String field;
    int participation;
    LogicalVariable lv;

    public Node(String field, int participation, LogicalVariable lv) {
        this.field = field;
        this.participation = participation;
        this.lv = lv;
    }
}
