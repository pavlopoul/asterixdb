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
package org.apache.hyracks.storage.am.statistics.dataflow;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FixedSizeFrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameOutputStream;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.io.FrameOutputStreamReader;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;

public class FrameOutputStreamWriter implements IFrameWriter {
    private FrameOutputStream frameOutputStream;
    private IHyracksTaskContext ctx;
    private boolean failed;
    protected FrameTupleAccessor accessor;
    FixedSizeFrame appendFrame;
    FixedSizeFrameTupleAppender appender;

    public FrameOutputStreamWriter(FrameOutputStream frameOutputStream, IHyracksTaskContext ctx) {
        this.frameOutputStream = frameOutputStream;
        this.ctx = ctx;
        appendFrame = new FixedSizeFrame();
        appender = new FixedSizeFrameTupleAppender();
    }

    @Override
    public void open() throws HyracksDataException {
        final IFrame frame = new VSizeFrame(ctx);

        frameOutputStream = new FrameOutputStream(ctx.getInitialFrameSize());
        frameOutputStream.reset(frame, true);
    }

    @Override
    public void fail() throws HyracksDataException {
        failed = true;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            processTuple(i);
        }
        //if (!frameOutputStream.appendTuple()) {
        frameOutputStream.appendTuple();
        //  }
    }

    private void processTuple(int tid) throws HyracksDataException {
        int size = accessor.getTupleLength(tid);
        int actualSize = FrameHelper.calcRequiredSpace(0, size);
        IDeallocatableFramePool framePool = new DeallocatableFramePool(ctx, 32768);
        int frameSize = FrameHelper.calcAlignedFrameSizeToStore(0, size, framePool.getMinFrameSize());
        ByteBuffer newBuffer = framePool.allocateFrame(frameSize);
        appendFrame.reset(newBuffer);
        appender.reset(appendFrame, true);
        //        int fid = getLastBufferOrCreateNewIfNotExist(partition, actualSize);
        //        if (fid < 0) {
        //            return false;
        //        }
        //        partitionArray[partition].getFrame(fid, tempInfo);
        //        int tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        //        if (tid < 0) {
        //            if (partitionArray[partition].getNumFrames() >= constrain.frameLimit(partition)) {
        //                return false;
        //            }
        //            fid = createNewBuffer(partition, actualSize);
        //            if (fid < 0) {
        //                return false;
        //            }
        //            partitionArray[partition].getFrame(fid, tempInfo);
        //            tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        //        }
        //        pointer.reset(makeGroupFrameId(partition, fid), tid);
        //        numTuples[partition]++;
        //        return true;
    }

    @Override
    public void close() throws HyracksDataException {
        //        if (!failed) {
        //            ioManager.close(handle);
        //        }
    }

    public FrameOutputStreamReader createReader() throws HyracksDataException {
        if (failed) {
            throw new HyracksDataException("createReader() called on a failed RunFileWriter");
        }
        return new FrameOutputStreamReader();
    }

    @Override
    public void flush() throws HyracksDataException {
        // this is a kind of a sink operator and hence, flush() is a no op
    }
}
