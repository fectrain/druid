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

import com.google.common.base.Supplier;
import org.apache.druid.collections.ResourceHolder;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class BlockLayoutColumnarLongsSupplier implements Supplier<ColumnarLongs>
{
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseLongBuffers;

  // The number of rows in this column.
  private final int totalSize;

  // The number of longs per buffer.
  private final int sizePer;
  private final CompressionFactory.LongEncodingReader baseReader;

  public BlockLayoutColumnarLongsSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder order,
      CompressionFactory.LongEncodingReader reader,
      CompressionStrategy strategy
  )
  {
    baseLongBuffers = GenericIndexed.read(fromBuffer, DecompressingByteBufferObjectStrategy.of(order, strategy));
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseReader = reader;
  }

  @Override
  public ColumnarLongs get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer); // 8192 - 13
    final int rem = sizePer - 1;
    final boolean isPowerOf2 = sizePer == (1 << div); // 这是对1 进行左移，又回到8192， 用这种方式判断是否是2的次方
    if (isPowerOf2) {
      // this provide slightly better performance than calling the LongsEncodingReader.read, probably because Java
      // doesn't inline the method call for some reason. This should be removed when test show that performance
      // of using read method is same as directly accessing the longbuffer
      if (baseReader instanceof LongsLongEncodingReader) { //看下performance 高在哪里？
        return new BlockLayoutColumnarLongs()
        {
          @Override
          public long get(int index) //8192 的话走这里，否则走下面分get() - Memory
          {
            // optimize division and remainder for powers of 2
            final int bufferNum = index >> div; // 蛤？哦哦 右移13位， bufferNum 为什么是final， 为什么不 ++？

            if (bufferNum != currBufferNum) {
              loadBuffer(bufferNum);
            }

            final int bufferIndex = index & rem;
            return longBuffer.get(bufferIndex); // 直接连baseReader.read() 都不需要了吗？
          }

          @Override
          protected void loadBuffer(int bufferNum)
          {
            if (holder != null) {
              holder.close();
            }
            holder = singleThreadedLongBuffers.get(bufferNum);
            buffer = holder.get();
            // asLongBuffer() makes the longBuffer's position = 0
            longBuffer = buffer.asLongBuffer();
            reader.setBuffer(buffer);
            currBufferNum = bufferNum;
          }
        };
      } else {
        return new BlockLayoutColumnarLongs()
        {
          @Override
          public long get(int index)
          {
            // optimize division and remainder for powers of 2
            final int bufferNum = index >> div;

            if (bufferNum != currBufferNum) {
              loadBuffer(bufferNum);
            }

            final int bufferIndex = index & rem;
            return reader.read(bufferIndex);
          }
        };
      }

    } else {
      return new BlockLayoutColumnarLongs();
    }
  }

  private class BlockLayoutColumnarLongs implements ColumnarLongs
  {
    final CompressionFactory.LongEncodingReader reader = baseReader.duplicate();
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedLongBuffers = baseLongBuffers.singleThreaded();

    int currBufferNum = -1;
    @Nullable
    ResourceHolder<ByteBuffer> holder;
    @Nullable
    ByteBuffer buffer;
    /**
     * longBuffer's position must be 0
     */
    @Nullable
    LongBuffer longBuffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public long get(int index)
    {
      // division + remainder is optimized by the compiler so keep those together
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currBufferNum) {
        loadBuffer(bufferNum);
      }

      return reader.read(bufferIndex);
    }

    @Override
    public void get(final long[] out, final int start, final int length)
    {
      // division + remainder is optimized by the compiler so keep those together
      int bufferNum = start / sizePer; // 8192
      int bufferIndex = start % sizePer; // 8192 中的位移 - 开始位置

      int p = 0;

      while (p < length) {
        if (bufferNum != currBufferNum) {
          loadBuffer(bufferNum);
        }

        final int limit = Math.min(length - p, sizePer - bufferIndex);
        reader.read(out, p, bufferIndex, limit);
        p += limit;
        bufferNum++;
        bufferIndex = 0;
      }
    }

    @Override
    public void get(final long[] out, final int[] indexes, final int length)
    { // 就是从 这么多个index 里面 读满length 长度， 因为index 是不连续的，可能散落在不同的 block， 每个block p 开始读到limit。 所以每个index 拿出来， 都要重新判断下 是否超过limit
      int p = 0;

      while (p < length) { // p=0, len = 100
        int bufferNum = indexes[p] / sizePer;
        if (bufferNum != currBufferNum) {
          loadBuffer(bufferNum);
        }

        final int numRead = reader.read(out, p, indexes, length - p, bufferNum * sizePer, sizePer);
        assert numRead > 0;
        p += numRead;
      }
    }

    protected void loadBuffer(int bufferNum)
    {
      if (holder != null) {
        holder.close();
      }
      holder = singleThreadedLongBuffers.get(bufferNum); // 这里解压
      buffer = holder.get();
      currBufferNum = bufferNum;
      reader.setBuffer(buffer); //这里是重复了？ 铁打的reader， 流水的buffer
    }

    @Override
    public void close()
    {
      if (holder != null) {
        currBufferNum = -1;
        holder.close();
        holder = null;
        buffer = null;
        longBuffer = null;
      }
    }

    @Override
    public String toString()
    {
      return "BlockCompressedColumnarLongs_Anonymous{" +
             "currBufferNum=" + currBufferNum +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedLongBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }
  }
}
