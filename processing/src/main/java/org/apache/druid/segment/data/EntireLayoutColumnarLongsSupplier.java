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

public class EntireLayoutColumnarLongsSupplier implements Supplier<ColumnarLongs>
{

  private final int totalSize;
  private final CompressionFactory.LongEncodingReader baseReader;

  public EntireLayoutColumnarLongsSupplier(int totalSize, CompressionFactory.LongEncodingReader reader)
  {
    this.totalSize = totalSize;
    this.baseReader = reader;
  }

  @Override
  public ColumnarLongs get()
  {
    if (baseReader instanceof TimestampDeltaEncodingReader) {
      return new EntireLayoutColumnarLongs()
      {
        @Override
        public void get(long[] out, int start, int length)
        {
          if (length <= 0) {
            return;
          }
          reader.read(out, 0, start, length);
        }

        @Override
        public void get(long[] out, int[] indexes, int length)
        {
          if (length <= 0) {
            return;
          }
          reader.read(out, 0, indexes, length, 0, totalSize);
        }
      };
    }
    return new EntireLayoutColumnarLongs()
    {};
  }

  private class EntireLayoutColumnarLongs implements ColumnarLongs
  {
    final CompressionFactory.LongEncodingReader reader = baseReader.duplicate();
    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public long get(int index)
    {
      return reader.read(index);
    }

    @Override
    public String toString()
    {
      return "EntireCompressedColumnarLongs_Anonymous{" +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void close()
    {
    }
  }
}
