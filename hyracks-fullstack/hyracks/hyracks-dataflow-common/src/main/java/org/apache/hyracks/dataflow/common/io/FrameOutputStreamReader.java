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
package org.apache.hyracks.dataflow.common.io;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FrameOutputStreamReader implements IFrameReader {
    // private FrameOutputStream frameOutputStream;

    public FrameOutputStreamReader() {
        //  this.frameOutputStream = frameOutputStream;
    }

    @Override
    public void open() throws HyracksDataException {
    }

    @Override
    public boolean nextFrame(IFrame frame) throws HyracksDataException {
        frame.reset();

        frame.ensureFrameSize(frame.getMinSize() * FrameHelper.deserializeNumOfMinFrame(frame.getBuffer()));
        if (frame.getBuffer().hasRemaining()) {
            if (frame.getBuffer().hasRemaining()) { // file is vanished.
                FrameHelper.clearRemainingFrame(frame.getBuffer(), frame.getBuffer().position());
            }
        }
        frame.getBuffer().flip();
        return true;
    }

    @Override
    public void close() throws HyracksDataException {
    }

}
