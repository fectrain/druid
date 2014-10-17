/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.examples.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.firehose.rabbitmq.JacksonifiedConnectionFactory;
import io.druid.firehose.rabbitmq.RabbitMQFirehoseConfig;
import io.druid.firehose.rabbitmq.RabbitMQFirehoseFactory;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class RabbitMQFirehoseFactoryTest
{
  private static final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    RabbitMQFirehoseConfig config = new RabbitMQFirehoseConfig(
        "test",
        "test2",
        "test3",
        true,
        true,
        true,
        5,
        10,
        20
    );

    JacksonifiedConnectionFactory connectionFactory = new JacksonifiedConnectionFactory(
        "foo",
        9978,
        "user",
        "pw",
        "host",
        null,
        5,
        10,
        11,
        12,
        ImmutableMap.<String, Object>of("hi", "bye")
    );

    RabbitMQFirehoseFactory factory = new RabbitMQFirehoseFactory(
        connectionFactory,
        config,
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("timestamp", "auto"),
                new DimensionsSpec(
                    Arrays.asList("dim"),
                    Lists.<String>newArrayList(),
                    Lists.<SpatialDimensionSchema>newArrayList()
                )
            ),
            null, null, null, null
        )
    );

    byte[] bytes = mapper.writeValueAsBytes(factory);
    RabbitMQFirehoseFactory factory2 = mapper.readValue(bytes, RabbitMQFirehoseFactory.class);
    byte[] bytes2 = mapper.writeValueAsBytes(factory2);

    Assert.assertArrayEquals(bytes, bytes2);

    Assert.assertEquals(factory.getConfig(), factory2.getConfig());
    Assert.assertEquals(factory.getConnectionFactory(), factory2.getConnectionFactory());
  }
}
