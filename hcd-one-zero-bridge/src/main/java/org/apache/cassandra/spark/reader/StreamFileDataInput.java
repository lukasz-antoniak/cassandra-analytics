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

package org.apache.cassandra.spark.reader;

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileDataInput;
import org.jetbrains.annotations.NotNull;

public class StreamFileDataInput implements FileDataInput
{
    private final DataInputPlus in;

    public StreamFileDataInput(@NotNull DataInputPlus in)
    {
        this.in = in;
    }

    @Override
    public File getFile()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEOF() throws IOException
    {
        return false;
    }

    @Override
    public long bytesRemaining() throws IOException
    {
        return -1;
    }

    @Override
    public void seek(long l) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFilePointer()
    {
        return -1;
    }

    @Override
    public DataPosition mark()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset(DataPosition dataPosition) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long bytesPastMark(DataPosition dataPosition)
    {
        return -1;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public void readFully(@NotNull byte[] b) throws IOException
    {
        in.readFully(b);
    }

    @Override
    public void readFully(@NotNull byte[] b, int off, int len) throws IOException
    {
        in.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int i) throws IOException
    {
        return in.skipBytes(i);
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return in.readBoolean();
    }

    @Override
    public byte readByte() throws IOException
    {
        return in.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        return in.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException
    {
        return in.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        return in.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException
    {
        return in.readChar();
    }

    @Override
    public int readInt() throws IOException
    {
        return in.readInt();
    }

    @Override
    public long readLong() throws IOException
    {
        return in.readLong();
    }

    @Override
    public float readFloat() throws IOException
    {
        return in.readFloat();
    }

    @Override
    public double readDouble() throws IOException
    {
        return in.readDouble();
    }

    @Override
    public String readLine() throws IOException
    {
        return in.readLine();
    }

    @NotNull
    @Override
    public String readUTF() throws IOException
    {
        return in.readUTF();
    }
}
