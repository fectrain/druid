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

import org.apache.druid.java.util.common.IAE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TimestampDeltaEncodingReader implements CompressionFactory.LongEncodingReader
{

  private ByteBuffer buffer;
  private final long base;

  public TimestampDeltaEncodingReader(ByteBuffer fromBuffer, ByteOrder order)
  {
    this.buffer = fromBuffer.asReadOnlyBuffer();
    byte version = buffer.get();
    if (version == CompressionFactory.TIMESTAMP_DELTA_ENCODING_VERSION) {
      base = buffer.getLong();
      fromBuffer.position(buffer.position()); // ??
    } else {
      throw new IAE("Unknown version[%s]", version);
    }
    this.buffer = this.buffer.slice(); // 把meta 之前的 byte丢掉
    this.buffer.order(order); // 为什么放在这里？
  }

  private TimestampDeltaEncodingReader(ByteBuffer buffer, ByteOrder order, long base)
  {
    this.buffer = buffer;
    this.buffer.order(order);
    this.base = base;
  }

  @Override
  public void setBuffer(ByteBuffer buffer) // 这里的setBuffer 有什么区别？
  {
    // todo: 把long[] 解析出来， 这样就不用每次get，如何能让每个block 只decode 一次？反序列化的时候？
    // 不能，这么搞， 500w个ts 还是不下的， 1个8byte 8*500 000 = 40MB， 同时查询100个segment，岂不是4GB， 怪不得查询会导致内存溢出，需要load file 到内存
    this.buffer = buffer.asReadOnlyBuffer();
    this.buffer.order(buffer.order());
  }

  @Override
  public long read(int index)
  {
    buffer.rewind(); // todo 测试读多次 rewind 过头了？
    int p = 0;
    long delta = 0;
    while (buffer.remaining() > 0 && p < index) {
      long val = buffer.getLong();
      if (val < 0) {
        p -= val;
      } else {
        delta += val;
        p++;
      }
    }
    return base + delta;
  }

  @Override
  public void read(final long[] out, int outPosition, int startIndex, int length)
  {
    buffer.rewind(); // 还是说 不能rewind？
    long prev = base;

    int p = 0, curIndex = 0;

    // 先对齐 startIndex, 将第一个position 放入
    while (buffer.remaining() > 0 && curIndex < startIndex) {
      long delta = buffer.getLong();
      if (delta < 0) {
        curIndex -= delta;
      } else {
        prev += delta;
        curIndex++;
      }
    }

    out[outPosition++] = prev;
    p++;

    int gap = curIndex - startIndex;
    while (gap > 0 && p < length) {
      out[outPosition++] = prev;
      gap--;
      p++;
    }

    // 补齐 startIndex 之后的
    while (buffer.remaining() > 0 && p < length) {
      long delta = buffer.getLong();
      if (delta < 0) {
        while (delta < 0 && p < length) {
          out[outPosition++] = prev;
          delta++;
          p++;
        }
      } else {
        out[outPosition] = delta + prev;
        prev = out[outPosition];
        outPosition++;
        p++;
      }
    }
  }

  @Override
  public int read(long[] out, int outPosition, int[] indexes, int length, int indexOffset, int limit)
  {
    // 从头开始读，读到最后一个吗？
    buffer.rewind();
    // 全部展开吧？试试benchmark
    long[] flatten = new long[limit];
    flatten[0] = base;
    int p = 1;
    while (buffer.remaining() > 0 && p < limit) {
      long delta = buffer.getLong();
      if (delta < 0) {
        for (int i = 0; i < -delta; i++) {
          flatten[p] = flatten[p - 1];
          p++;
        }
      } else {
        flatten[p] = flatten[p - 1] + delta;
        p++;
      }
    }
    for (int i = 0; i < length; i++) {
      int index = indexes[outPosition + i] - indexOffset;
      if (index >= limit) {
        return i;
      }
      out[outPosition + i] = flatten[i];
    }
    return length;
  }

  @Override
  public CompressionFactory.LongEncodingReader duplicate()
  {
    return new TimestampDeltaEncodingReader(buffer.duplicate(), buffer.order(), base);
  }
}
