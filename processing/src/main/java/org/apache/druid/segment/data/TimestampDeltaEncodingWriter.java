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

package org.apache.druid.segment.data;

import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class TimestampDeltaEncodingWriter implements CompressionFactory.LongEncodingWriter
{

  private final ByteBuffer orderBuffer;
  private final ByteOrder order;
  @Nullable
  private ByteBuffer outBuffer = null;
  @Nullable
  private OutputStream outStream = null;

  private int sizePer;

  private long base = -1;

  private long prev = -1;

  private int zeroCnt = 0;

  private int encodedSize = 0;

  public TimestampDeltaEncodingWriter(ByteOrder order, int sizePer)
  {
    this.order = order;
    this.sizePer = sizePer;
//    System.out.println("**sizePer: "+ sizePer);
    orderBuffer = ByteBuffer.allocate(Integer.BYTES * 2);
    orderBuffer.order(order);
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    outStream = null;
    outBuffer = buffer;
    // this order change is safe as the buffer is passed in and allocated in BlockLayoutColumnarLongsSerializer, and
    // is used only as a temporary storage to be written
    outBuffer.order(order);
  }

  @Override
  public void setOutputStream(WriteOutBytes output) // 只有 entireLayout -
  {
    outBuffer = null;
    outStream = output;
  }

  @Override
  public void write(long value) throws IOException
  {
    if (outBuffer != null) {
      processVal(outBuffer, value);
    }
    if (outStream != null) {
      orderBuffer.clear();
      processVal(orderBuffer, value);
      // 3种情况， 空 / 放一个 / 放两个
      orderBuffer.flip();
      if (orderBuffer.remaining() > 0) {
        outStream.write(Arrays.copyOfRange(orderBuffer.array(), orderBuffer.position(), orderBuffer.limit()));
      }
    }
  }

  private void processVal(ByteBuffer buf, long value)
  {
    if (base == -1) {
      base = value;
      prev = base;
      buf.putInt(0);
      return;
    }

    if (buf.position() == 0) {
      long baseDelta = value - base;
      buf.putInt((int)baseDelta);
      prev = value;
      return;
    }

    if (value == prev) {
      zeroCnt++;
    } else {
      if (zeroCnt != 0) {
        buf.putInt(-zeroCnt);
        zeroCnt = 0;
      }
      buf.putInt((int)(value - prev));
      encodedSize++;
      prev = value;
    }
  }

  @Override
  public void flush() throws IOException
  {
    if (zeroCnt != 0) {
      if (outBuffer != null) {
        outBuffer.putInt(-zeroCnt);
      }
      if (outStream != null) {
        orderBuffer.clear();
        orderBuffer.putInt(-zeroCnt);
        orderBuffer.flip();
        outStream.write(Arrays.copyOfRange(orderBuffer.array(), orderBuffer.position(), orderBuffer.limit()));
      }
      zeroCnt = 0;
    }
  }

  @Override
  public void putMeta(ByteBuffer metaOut, CompressionStrategy strategy)
  {
    metaOut.put(CompressionFactory.setEncodingFlag(strategy.getId()));
    metaOut.put(CompressionFactory.LongEncodingFormat.TIMEATAMP_DELTA.getId());
    metaOut.put(CompressionFactory.TIMESTAMP_DELTA_ENCODING_VERSION);
    metaOut.putLong(base);
  }

  @Override
  public int metaSize()
  {
    return 1 + 1 + 1 + Long.BYTES;
  }

  @Override
  public int getBlockSize(int bytesPerBlock)
  {
    // unrelevant with block bytes
//    return bytesPerBlock / Integer.BYTES; // 唯一的区别就是这里了？sizePer 大小固定
    return sizePer <= 0 ? bytesPerBlock / Integer.BYTES : sizePer;
  }

  @Override
  public int getNumBytes(int values)
  {
    return values * Integer.BYTES; //只要是以long 形式存储的就是
  }

  @Override
  public int getEncodedSize()
  {
    return encodedSize;
  }
}
