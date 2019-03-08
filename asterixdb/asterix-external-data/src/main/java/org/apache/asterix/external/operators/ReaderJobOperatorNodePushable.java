package org.apache.asterix.external.operators;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class ReaderJobOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private final ReaderJobOperatorDescriptor opDesc;

    public ReaderJobOperatorNodePushable(IHyracksTaskContext ctx, ReaderJobOperatorDescriptor readerJob) {
        opDesc = readerJob;

    }

}
