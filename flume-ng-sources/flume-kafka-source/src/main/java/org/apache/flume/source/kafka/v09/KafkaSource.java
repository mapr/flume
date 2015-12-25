/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.kafka.v09;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.List;
import java.util.UUID;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.flume.source.AbstractPollableSource;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Source for Kafka which reads messages from kafka topics.
 *
 * <tt>kafka.bootstrap.servers: </tt> Kafka's bootstrap servers addresses.
 * <b>Required</b> for kafka.
 * <p>
 * <tt>kafka.consumer.group.id: </tt> the group ID of consumer group. <b>Required</b>
 * <p>
 * <tt>kafka.topics: </tt> the topic list separated by commas to consume messages from.
 * <b>Required</b>
 * <p>
 * <tt>maxBatchSize: </tt> Maximum number of messages written to Channel in one
 * batch. Default: 1000
 * <p>
 * <tt>maxBatchDurationMillis: </tt> Maximum number of milliseconds before a
 * batch (of any size) will be written to a channel. Default: 1000
 * <p>
 * <tt>kafka.consumer.*: </tt> Any property starting with "kafka.consumer" will be
 * passed to the kafka consumer So you can use any configuration supported by Kafka 0.9.0.0
 * <p>
 */
public class KafkaSource extends AbstractPollableSource
        implements Configurable {
  private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);

  private Context context;
  private Properties kafkaProps;
  private KafkaSourceCounter counter;
  private KafkaConsumer<String, byte[]> consumer;
  private Iterator<ConsumerRecord<String, byte[]>> it;

  private final List<Event> eventList = new ArrayList<Event>();
  private Map<TopicPartition, OffsetAndMetadata> toBeCommitted;
  private AtomicBoolean rebalanceFlag;

  private Map<String, String> headers;

  private int batchUpperLimit;
  private int maxBatchDurationMillis;

  private List<String> topicList;
  private boolean isStreams;

  /**
   * Lock critical section of code to be executed by one thread
   * at the same time in order not to get a corrupted state when
   * stopping kafka source.
   */
  private Lock lock;

  @Override
  protected Status doProcess() throws EventDeliveryException {
    final String batchUUID = UUID.randomUUID().toString();
    byte[] kafkaMessage;
    String kafkaKey;
    Event event;

    try {
      // prepare time variables for new batch
      final long nanoBatchStartTime = System.nanoTime();
      final long batchStartTime = System.currentTimeMillis();
      final long batchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;

      while (eventList.size() < batchUpperLimit &&
              System.currentTimeMillis() < batchEndTime) {

        if (it == null || !it.hasNext()) {
          // Obtaining new records
          // Poll time is remainder time for current batch.
          ConsumerRecords<String, byte[]> records = consumer.poll(
                  Math.max(0, batchEndTime - System.currentTimeMillis()));
          it = records.iterator();

          // this flag is set to true in a callback when some partitions are revoked.
          // If there are any records we commit them.
          if (rebalanceFlag.get()) {
            rebalanceFlag.set(false);
            break;
          }
          // check records after poll
          if (!it.hasNext()) {
            if (log.isDebugEnabled()) {
              counter.incrementKafkaEmptyCount();
              log.debug("Returning with backoff. No more data to read");
            }
            // batch time exceeded
            break;
          }
        }

        // get next message
        ConsumerRecord<String, byte[]> message = it.next();
        kafkaMessage = message.value();
        kafkaKey = message.key();

        // Add headers to event (timestamp, topic, partition, key)
        headers.put(KafkaSourceConstants.TIMESTAMP_HEADER, String.valueOf(System.currentTimeMillis()));
        headers.put(KafkaSourceConstants.TOPIC_HEADER, message.topic());
        headers.put(KafkaSourceConstants.PARTITION_HEADER, String.valueOf(message.partition()));
        if (kafkaKey != null) {
          headers.put(KafkaSourceConstants.KEY_HEADER, kafkaKey);
        }

        if (log.isDebugEnabled()) {
          log.debug("Message: {}", new String(kafkaMessage));
          log.debug("Topic: {} Partition: {}", message.topic(), message.partition());
        }

        event = EventBuilder.withBody(kafkaMessage, headers);
        eventList.add(event);

        if (log.isDebugEnabled()) {
          log.debug("Waited: {} ", System.currentTimeMillis() - batchStartTime);
          log.debug("Event #: {}", eventList.size());
        }

        // For MapR Streams we need to commit offset of record X (not X+1 as for Kafka)
        // when we want to fetch a record X+1 for the next poll after rebalance.
        long offset = isStreams ? message.offset() : message.offset() + 1;
        toBeCommitted.put(new TopicPartition(message.topic(), message.partition()),
                new OffsetAndMetadata(offset, batchUUID));
      }

      if (eventList.size() > 0) {
        counter.addToKafkaEventGetTimer((System.nanoTime() - nanoBatchStartTime) / (1000 * 1000));
        counter.addToEventReceivedCount((long) eventList.size());
        getChannelProcessor().processEventBatch(eventList);
        counter.addToEventAcceptedCount(eventList.size());
        if (log.isDebugEnabled()) {
          log.debug("Wrote {} events to channel", eventList.size());
        }
        eventList.clear();
        // commit must not be interrupted when agent stops.
        try {
          lock.lock();
          long commitStartTime = System.nanoTime();
          consumer.commitSync(toBeCommitted);
          long commitEndTime = System.nanoTime();
          counter.addToKafkaCommitTimer((commitEndTime - commitStartTime) / (1000 * 1000));
          toBeCommitted.clear();
        } finally {
          lock.unlock();
        }
        return Status.READY;
      }

      return Status.BACKOFF;
    } catch (Exception e) {
      log.error("KafkaSource EXCEPTION, {}", e);
      return Status.BACKOFF;
    }
  }

  /**
   * We configure the source and generate properties for the Kafka Consumer
   *
   * Kafka Consumer properties are generated as follows:
   * 1. Generate a properties object with some static defaults that can be
   * overridden by Source configuration
   * 2. We add the configuration users added for Kafka (parameters starting
   * with kafka. and must be valid Kafka Consumer
   * properties
   * 3. We add the source documented parameters which can override other properties
   *
   * @param context
   */
  @Override
  protected void doConfigure(Context context) throws FlumeException {
    this.context = context;
    headers = new HashMap<String, String>(4);
    toBeCommitted = new HashMap<TopicPartition, OffsetAndMetadata>();
    lock = new ReentrantLock();
    rebalanceFlag = new AtomicBoolean(false);

    String topics = context.getString(KafkaSourceConstants.TOPICS);
    if(topics == null) {
      throw new ConfigurationException("At least one Kafka topic must be specified.");
    }
    // Subscribe to multiple topics.
    topicList = Arrays.asList(topics.split("^\\s+|\\s*,\\s*|\\s+$"));

    // When all specified topics start with
    // slash then MapR Streams are used
    isStreams = true;
    for (String topic : topicList) {
      if (!topic.startsWith("/")) {
        isStreams = false;
        break;
      }
    }
    batchUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_SIZE,
            KafkaSourceConstants.DEFAULT_BATCH_SIZE);
    maxBatchDurationMillis = context.getInteger(KafkaSourceConstants.BATCH_DURATION_MS,
            KafkaSourceConstants.DEFAULT_BATCH_DURATION);

    String bootstrapServers = context.getString(KafkaSourceConstants.BOOTSTRAP_SERVERS);

    setConsumerProps(context, bootstrapServers);

    if (counter == null) {
      counter = new KafkaSourceCounter(getName());
    }
  }

  private void setConsumerProps(Context ctx, String bootStrapServers) {
    kafkaProps = new Properties();
    String groupId = ctx.getString(KafkaSourceConstants.KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG);
    if (groupId == null || groupId.isEmpty()) {
      groupId = KafkaSourceConstants.DEFAULT_GROUP_ID;
      log.info("Group ID was not specified. Using " + groupId + " as the group id.");
    }
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaSourceConstants.DEFAULT_KEY_DESERIALIZER);
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSourceConstants.DEFAULT_VALUE_DESERIAIZER);
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaSourceConstants.DEFAULT_GROUP_ID);
    //Defaults overridden based on config
    kafkaProps.putAll(ctx.getSubProperties(KafkaSourceConstants.KAFKA_CONSUMER_PREFIX));
    //These always take precedence over config
    if (bootStrapServers != null)
      kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaSourceConstants.DEFAULT_AUTO_COMMIT);

    log.info(kafkaProps.toString());
  }

  Properties getConsumerProps() {
    return kafkaProps;
  }

  @Override
  protected void doStart() throws FlumeException {
    log.info("Starting {}...", this);

    try {
      //initialize a consumer.
      consumer = new KafkaConsumer<String, byte[]>(kafkaProps);
    } catch (Exception e) {
      throw new FlumeException("Unable to create consumer. " +
              "Check whether the Bootstrap server is up and that the " +
              "Flume agent can connect to it.", e);
    }

    // We can use topic subscription or partition assignment strategy.
    consumer.subscribe(topicList, new SourceRebalanceListener(rebalanceFlag));

    // Connect to kafka. 1 second is optimal time.
    it = consumer.poll(1000).iterator();
    log.info("Kafka source {} started.", getName());
    counter.start();
  }

  @Override
  protected void doStop() throws FlumeException {
    if (consumer != null) {
      try {
        lock.lock();
        consumer.wakeup();
        consumer.close();
      } finally {
        lock.unlock();
      }
    }
    counter.stop();
    log.info("Kafka Source {} stopped. Metrics: {}", getName(), counter);
  }
}


class SourceRebalanceListener implements ConsumerRebalanceListener {
  private static final Logger log = LoggerFactory.getLogger(SourceRebalanceListener.class);
  private AtomicBoolean rebalanceFlag;

  public SourceRebalanceListener(AtomicBoolean rebalanceFlag) {
    this.rebalanceFlag = rebalanceFlag;
  }

  // Set a flag that a rebalance has occurred. Then commit already read events to kafka.
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    for (TopicPartition partition : partitions) {
      log.info("topic {} - partition {} revoked.", partition.topic(), partition.partition());
      rebalanceFlag.set(true);
    }
  }

  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    for (TopicPartition partition : partitions) {
      log.info("topic {} - partition {} assigned.", partition.topic(), partition.partition());
    }
  }
}
