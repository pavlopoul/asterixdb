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
package org.apache.asterix.optimizer.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushFieldNameToDataScanRule implements IAlgebraicRewriteRule {
    static DataSourceScanOperator scan;
    static AssignOperator assign;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        Mutable<ILogicalExpression> f = getFunctionCall(opRef, context);
        if (f == null) {
            return false;
        }
        //        ILogicalExpression expr = f.getArguments().get(1).getValue();
        //        if (expr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
        //            return false;
        //        }
        //        ConstantExpression ce = (ConstantExpression) expr;
        //        IAlgebricksConstantValue acv = ce.getValue();
        //        if (!(acv instanceof AsterixConstantValue)) {
        //            return false;
        //        }
        //        AsterixConstantValue acv2 = (AsterixConstantValue) acv;
        //        int pos = ((AInt32) acv2.getObject()).getIntegerValue();
        //        ARecordType rt = getRecordType(getDataset(context), (MetadataProvider) context.getMetadataProvider());
        //        String fldName = rt.getFieldNames()[pos];
        //        DataSource ds = (DataSource) scan.getDataSource();
        //        ds.getId().addFieldName(fldName);
        //        int size = ds.getSchemaTypes().length;
        //        IAType[] schemaTypes = ds.getSchemaTypes();
        //        schemaTypes[size - 1] = rt.getFieldTypes()[pos];
        scan.setExtendedDataSource(f);
        scan.getScanVariables().add(context.newVar());
        VariableReferenceExpression vaExp = new VariableReferenceExpression(scan.getScanVariables().get(2));
        ILogicalExpression le = vaExp.cloneExpression();
        //
        AssignOperator noOp =
                new AssignOperator(assign.getVariables().get(0), new MutableObject<ILogicalExpression>(le));
        noOp.getInputs().addAll(assign.getInputs());
        opRef.setValue(noOp);
        //        opRef.setValue(scan);
        OperatorPropertiesUtil.typeOpRec(opRef, context);
        return true;
    }

    public static Mutable<ILogicalExpression> getFunctionCall(Mutable<ILogicalOperator> opRef,
            IOptimizationContext context) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return null;
        }
        assign = (AssignOperator) op;
        Set<LogicalVariable> assignVariables = new HashSet<>();
        assign.getExpressions().get(0).getValue().getUsedVariables(assignVariables);
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) assign.getInputs().get(0).getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return null;
        }
        scan = getScan(op1);
        if (scan.getVariables().size() == 1)
            return null;
        ILogicalExpression assignExpr = assign.getExpressions().get(0).getValue();
        if (assignExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        Mutable<ILogicalExpression> mle = assign.getExpressions().get(0);
        return mle;
    }

    public static ARecordType getRecordType(Dataset dataset, MetadataProvider mp) throws AlgebricksException {
        String tName = dataset.getItemTypeName();
        IAType t = mp.findType(dataset.getItemTypeDataverseName(), tName);
        ARecordType rt = (ARecordType) t;
        return rt;

    }

    public static Dataset getDataset(IOptimizationContext context) throws AlgebricksException {
        IDataSource<DataSourceId> dataSource = (IDataSource<DataSourceId>) scan.getDataSource();
        DataSourceId asid = dataSource.getId();
        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
        return mp.findDataset(asid.getDataverseName(), asid.getDatasourceName());
    }

    public static DataSourceScanOperator getScan(AbstractLogicalOperator op1) {
        return (DataSourceScanOperator) op1;
    }

}