/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.flume.sink.kafka.v09;

import com.google.common.base.Throwables;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * A Flume Sink that can publish messages to Kafka.
 * This is a general implementation that can be used with any Flume agent and
 * a channel.
 * The message can be any event and the key is a string that we read from the
 * header
 * For use of partitioning, use an interceptor to generate a header with the
 * partition key
 * <p/>
 * Mandatory properties are:
 * brokerList -- can be a partial list, but at least 2 are recommended for HA
 * <p/>
 * <p/>
 * however, any property starting with "kafka." will be passed along to the
 * Kafka producer
 * Read the Kafka producer documentation to see which configurations can be used
 * <p/>
 * Optional properties
 * topic - there's a default, and also - this can be in the event header if
 * you need to support events with
 * different topics
 * batchSize - how many messages to process in one batch. Larger batches
 * improve throughput while adding latency.
 * requiredAcks -- 0 (unsafe), 1 (accepted by at least one broker, default),
 * -1 (accepted by all brokers)
 * <p/>
 * header properties (per event):
 * topic
 * key
 */
public class KafkaSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);

  private final Properties kafkaProps = new Properties();
  private KafkaProducer<String, byte[]> producer;
  private String topic;
  private int batchSize;
  private List<Future<RecordMetadata>> kafkaFutures;
  private KafkaSinkCounter counter;


  @Override
  public Status process() throws EventDeliveryException {
    Status result = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = null;
    Event event = null;
    String eventTopic = null;
    String eventKey = null;

    try {
      long processedEvents = 0;

      transaction = channel.getTransaction();
      transaction.begin();

      kafkaFutures.clear();
      long batchStartTime = System.nanoTime();
      for (; processedEvents < batchSize; processedEvents += 1) {
        event = channel.take();

        if (event == null) {
          // no events available in channel
          if(processedEvents == 0) {
            result = Status.BACKOFF;
            counter.incrementBatchEmptyCount();
          } else {
            counter.incrementBatchUnderflowCount();
          }
          break;
        }

        byte[] eventBody = event.getBody();
        Map<String, String> headers = event.getHeaders();

        eventTopic = headers.get(KafkaSinkConstants.TOPIC_HEADER);
        if (eventTopic == null) {
          eventTopic = topic;
        }
        eventKey = headers.get(KafkaSinkConstants.KEY_HEADER);

        if (logger.isDebugEnabled()) {
          logger.debug("{Event} " + eventTopic + " : " + eventKey + " : "
            + new String(eventBody, "UTF-8"));
          logger.debug("event #{}", processedEvents);
        }

        // create a message and add to buffer
        long startTime = System.currentTimeMillis();
        kafkaFutures.add(producer.send(new ProducerRecord<String, byte[]> (eventTopic, eventKey, eventBody),
                                         new SinkCallback(startTime)));
      }

      //Prevent linger.ms from holding the batch
      producer.flush();

      // publish batch and commit.
      if (processedEvents > 0) {
          for (Future<RecordMetadata> future : kafkaFutures) {
            future.get();
          }
        long endTime = System.nanoTime();
        counter.addToKafkaEventSendTimer((endTime-batchStartTime)/(1000*1000));
        counter.addToEventDrainSuccessCount(Long.valueOf(kafkaFutures.size()));
      }

      transaction.commit();

    } catch (Exception ex) {
      String errorMsg = "Failed to publish events";
      logger.error("Failed to publish events", ex);
      result = Status.BACKOFF;
      if (transaction != null) {
        try {
          kafkaFutures.clear();
          transaction.rollback();
          counter.incrementRollbackCount();
        } catch (Exception e) {
          logger.error("Transaction rollback failed", e);
          throw Throwables.propagate(e);
        }
      }
      throw new EventDeliveryException(errorMsg, ex);
    } finally {
      if (transaction != null) {
        transaction.close();
      }
    }

    return result;
  }

  @Override
  public synchronized void start() {
    // instantiate the producer
    producer = new KafkaProducer<String,byte[]>(kafkaProps);
    counter.start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    producer.close();
    counter.stop();
    logger.info("Kafka Sink {} stopped. Metrics: {}", getName(), counter);
    super.stop();
  }


  /**
   * We configure the sink and generate properties for the Kafka Producer
   *
   * Kafka producer properties is generated as follows:
   * 1. We generate a properties object with some static defaults that
   * can be overridden by Sink configuration
   * 2. We add the configuration users added for Kafka (parameters starting
   * with .kafka. and must be valid Kafka Producer properties
   * 3. We add the sink's documented parameters which can override other
   * properties
   *
   * @param context
   */
  @Override
  public void configure(Context context) {

    String topicStr = context.getString(KafkaSinkConstants.TOPIC_CONFIG);
    if (topicStr == null || topicStr.isEmpty()) {
      topicStr = KafkaSinkConstants.DEFAULT_TOPIC;
      logger.warn("Topic was not specified. Using {} as the topic.", topicStr);
    }
    else {
      logger.info("Using the static topic {}. This may be overridden by event headers", topicStr);
    }

    topic = topicStr;

    batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE, KafkaSinkConstants.DEFAULT_BATCH_SIZE);

    if (logger.isDebugEnabled()) {
      logger.debug("Using batch size: {}", batchSize);
    }

    kafkaFutures = new LinkedList<Future<RecordMetadata>>();

    String bootStrapServers = context.getString(KafkaSinkConstants.BOOTSTRAP_SERVERS_CONFIG);

    setProducerProps(context, bootStrapServers);

    if (logger.isDebugEnabled()) {
      logger.debug("Kafka producer properties: {}" , kafkaProps);
    }

    if (counter == null) {
      counter = new KafkaSinkCounter(getName());
    }
  }

  private void setProducerProps(Context context, String bootStrapServers) {
    kafkaProps.put(ProducerConfig.ACKS_CONFIG, KafkaSinkConstants.DEFAULT_ACKS);
    //Defaults overridden based on config
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaSinkConstants.DEFAULT_KEY_SERIALIZER);
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaSinkConstants.DEFAULT_VALUE_SERIAIZER);
    kafkaProps.putAll(context.getSubProperties(KafkaSinkConstants.KAFKA_PRODUCER_PREFIX));
    if (bootStrapServers != null)
      kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    logger.info("Producer properties: {}" , kafkaProps.toString());
  }

  protected Properties getKafkaProps() {
    return kafkaProps;
  }
}

class SinkCallback implements Callback {
  private static final Logger logger = LoggerFactory.getLogger(SinkCallback.class);
  private long startTime;

  public SinkCallback(long startTime) {
    this.startTime = startTime;
  }

  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      logger.debug("Error sending message to Kafka {} ", exception.getMessage());
    }

    if (logger.isDebugEnabled()) {
      long eventElapsedTime = System.currentTimeMillis() - startTime;
      logger.debug("Acked message partition:{} ofset:{}",  metadata.partition(), metadata.offset());
      logger.debug("Elapsed time for send: {}", eventElapsedTime);
    }
  }
}

