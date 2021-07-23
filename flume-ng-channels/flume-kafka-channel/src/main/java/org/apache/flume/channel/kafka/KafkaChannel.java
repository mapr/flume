/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import kafka.zk.KafkaZkClient;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channel.kafka.KafkaChannel.ConsumerAndRecords;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaChannelCounter;
import org.apache.flume.shared.kafka.KafkaSSLUtil;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.*;

public class KafkaChannel extends BasicChannelSemantics {

  private static final Logger logger =
          LoggerFactory.getLogger(KafkaChannel.class);

  // Constants used only for offset migration zookeeper connections
  private static final int ZK_SESSION_TIMEOUT = 30000;
  private static final int ZK_CONNECTION_TIMEOUT = 30000;

  private final Properties consumerProps = new Properties();
  private final Properties producerProps = new Properties();

  private KafkaProducer<String, byte[]> producer;
  private final String channelUUID = UUID.randomUUID().toString();

//  private AtomicReference<String> topic = new AtomicReference<String>();
  private AtomicReference<List<String>> topics = new AtomicReference<>(Collections.EMPTY_LIST);
  private AtomicReference<Pattern> topicsPattern = new AtomicReference<>();

  private boolean parseAsFlumeEvent = DEFAULT_PARSE_AS_FLUME_EVENT;
  private String zookeeperConnect = null;
  private String groupId = DEFAULT_GROUP_ID;
  private String partitionHeader = null;
  private Integer staticPartitionId;
  @Deprecated
  private boolean migrateZookeeperOffsets = DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS;

  // This isn't a Kafka property per se, but we allow it to be configurable
  private long pollTimeout = DEFAULT_POLL_TIMEOUT;


  // Track all consumers to close them eventually.
  private final List<ConsumerAndRecords> consumers =
          Collections.synchronizedList(new LinkedList<ConsumerAndRecords>());

  private KafkaChannelCounter counter;

  /* Each Consumer commit will commit all partitions owned by it. To
  * ensure that each partition is only committed when all events are
  * actually done, we will need to keep a Consumer per thread.
  */

  private final ThreadLocal<ConsumerAndRecords> consumerAndRecords =
      new ThreadLocal<ConsumerAndRecords>() {
        @Override
        public ConsumerAndRecords initialValue() {
          return createConsumerAndRecords();
        }
      };

  @Override
  public void start() {
    logger.info("Starting Kafka Channel: {}", getName());
    // As a migration step check if there are any offsets from the group stored in kafka
    // If not read them from Zookeeper and commit them to Kafka
    if (migrateZookeeperOffsets && zookeeperConnect != null && !zookeeperConnect.isEmpty()) {
      migrateOffsets();
    }
    producer = new KafkaProducer<String, byte[]>(producerProps);
    // We always have just one topic being read by one thread
    if (topicsPattern.get() != null) {
      logger.info("Topics pattern = {}", topicsPattern.get().pattern());
    } else {
      logger.info("Topics = {}", topics.get());
    }

    counter.start();
    super.start();
  }

  @Override
  public void stop() {
    for (ConsumerAndRecords c : consumers) {
      try {
        decommissionConsumerAndRecords(c);
      } catch (Exception ex) {
        logger.warn("Error while shutting down consumer.", ex);
      }
    }
    producer.close();
    counter.stop();
    super.stop();
    logger.info("Kafka channel {} stopped.", getName());
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new KafkaTransaction();
  }

  @Override
  public void configure(Context ctx) {

    // Can remove in the next release
    translateOldProps(ctx);
    String topicsStr = ctx.getString(TOPICS_REGEX);
    if (topicsStr != null && !topicsStr.isEmpty()) {
      topicsPattern.set(Pattern.compile(topicsStr));
      logger.warn("Topics regex - '{}' is specified. " +
              "This channel can be used only for consuming data.", topicsStr);
    } else if ((topicsStr = ctx.getString(TOPICS)) != null &&
            !topicsStr.isEmpty()) {
      // Parsing in accordance to how it is done in KafkaSource
      List<String> topicsList = Arrays.asList(topicsStr.split("^\\s+|\\s*,\\s*|\\s+$"));
      topics.set(topicsList);

      if (topicsList.size() > 1) {
        logger.warn("There are multiple topics specified. " +
                "And for publishing data only first one - {} will be used.", topicsList.get(0));
      }
    } else if ((topicsStr = ctx.getString(TOPIC_CONFIG)) != null &&
            !topicsStr.isEmpty()) {
      topics.set(Arrays.asList(topicsStr));
    } else {
      topics.set(Arrays.asList(DEFAULT_TOPIC));
      logger.info("Topic(s) was not specified. Using {} as the topic.", DEFAULT_TOPIC);
    }

    groupId = ctx.getString(KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG);
    if (groupId == null || groupId.isEmpty()) {
      groupId = DEFAULT_GROUP_ID;
      logger.info("Group ID was not specified. Using {} as the group id.", groupId);
    }

    String bootStrapServers = null;

    if (!isStreams(ctx)) {
      bootStrapServers = ctx.getString(BOOTSTRAP_SERVERS_CONFIG);
      if (bootStrapServers == null || bootStrapServers.isEmpty()) {
        throw new ConfigurationException("Bootstrap Servers must be specified");
      }
    }

    setProducerProps(ctx, bootStrapServers);
    setConsumerProps(ctx, bootStrapServers);

    parseAsFlumeEvent = ctx.getBoolean(PARSE_AS_FLUME_EVENT, DEFAULT_PARSE_AS_FLUME_EVENT);
    pollTimeout = ctx.getLong(POLL_TIMEOUT, DEFAULT_POLL_TIMEOUT);

    staticPartitionId = ctx.getInteger(STATIC_PARTITION_CONF);
    partitionHeader = ctx.getString(PARTITION_HEADER_NAME);

    migrateZookeeperOffsets = ctx.getBoolean(MIGRATE_ZOOKEEPER_OFFSETS,
      DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS);
    zookeeperConnect = ctx.getString(ZOOKEEPER_CONNECT_FLUME_KEY);

    if (logger.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
      logger.debug("Kafka properties: {}", ctx);
    }

    if (counter == null) {
      counter = new KafkaChannelCounter(getName());
    }
  }

  // We can remove this once the properties are officially deprecated
  private void translateOldProps(Context ctx) {

    if (!(ctx.containsKey(TOPIC_CONFIG))
            && !(ctx.containsKey(TOPICS))
            && !(ctx.containsKey(TOPICS_REGEX))) {
      ctx.put(TOPIC_CONFIG, ctx.getString("topic"));
      logger.warn("{} is deprecated. Please use the parameter {}", "topic", TOPIC_CONFIG);
    }

    if (!isStreams(ctx)) {
      // Broker List
      // If there is no value we need to check and set the old param and log a warning message
      if (!(ctx.containsKey(BOOTSTRAP_SERVERS_CONFIG))) {
        String brokerList = ctx.getString(BROKER_LIST_FLUME_KEY);
        if (brokerList == null || brokerList.isEmpty()) {
          throw new ConfigurationException("Bootstrap Servers must be specified");
        } else {
          ctx.put(BOOTSTRAP_SERVERS_CONFIG, brokerList);
        }
        logger.warn("{} is deprecated. Please use the parameter {}",
                BROKER_LIST_FLUME_KEY, BOOTSTRAP_SERVERS_CONFIG);
      }
    }

    // GroupId
    // If there is an old Group Id set, then use that if no groupId is set.
    if (!(ctx.containsKey(KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG))) {
      String oldGroupId = ctx.getString(GROUP_ID_FLUME);
      if (oldGroupId != null  && !oldGroupId.isEmpty()) {
        ctx.put(KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG, oldGroupId);
        logger.warn("{} is deprecated. Please use the parameter {}",
                    GROUP_ID_FLUME, KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG);
      }
    }

    if (!(ctx.containsKey((KAFKA_CONSUMER_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)))) {
      Boolean oldReadSmallest = ctx.getBoolean(READ_SMALLEST_OFFSET);
      String auto;
      if (oldReadSmallest != null) {
        if (oldReadSmallest) {
          auto = "earliest";
        } else {
          auto = "latest";
        }
        ctx.put(KAFKA_CONSUMER_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,auto);
        logger.warn("{} is deprecated. Please use the parameter {}",
                    READ_SMALLEST_OFFSET,
                    KAFKA_CONSUMER_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
      }

    }
  }

  private boolean isStreams(Context ctx) {
    if (ctx.containsKey(TOPIC_CONFIG)
            && (ctx.getString(TOPIC_CONFIG) != null)) {
      return ctx.getString(TOPIC_CONFIG).startsWith("/");
    } else {
      if (ctx.containsKey(TOPICS_REGEX)) {
        return ctx.getString(TOPICS_REGEX).startsWith("/");
      } else {
        return ctx.getString(TOPICS).startsWith("/");
      }
    }
  }

  private void setProducerProps(Context ctx, String bootStrapServers) {
    producerProps.clear();
    producerProps.put(ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIAIZER);
    // Defaults overridden based on config
    producerProps.putAll(ctx.getSubProperties(KAFKA_PRODUCER_PREFIX));
    if (bootStrapServers != null) {
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    }

    KafkaSSLUtil.addGlobalSSLParameters(producerProps);
  }

  protected Properties getProducerProps() {
    return producerProps;
  }

  private void setConsumerProps(Context ctx, String bootStrapServers) {
    consumerProps.clear();
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIAIZER);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET);
    // Defaults overridden based on config
    consumerProps.putAll(ctx.getSubProperties(KAFKA_CONSUMER_PREFIX));
    // These always take precedence over config
    if (bootStrapServers != null) {
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    }
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    KafkaSSLUtil.addGlobalSSLParameters(consumerProps);

    logger.info(consumerProps.toString());
  }

  protected Properties getConsumerProps() {
    return consumerProps;
  }

  private synchronized ConsumerAndRecords createConsumerAndRecords() {
    try {
      KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(consumerProps);
      Lock rebalanceLock = new ReentrantLock(true);

      ConsumerAndRecords car = new ConsumerAndRecords(consumer, channelUUID, rebalanceLock);
      logger.info("Created new consumer to connect to Kafka");

      if (topicsPattern.get() != null) {
        car.consumer.subscribe(topicsPattern.get(), new ChannelRebalanceListener(car));
      } else {
        car.consumer.subscribe(topics.get(), new ChannelRebalanceListener(car));
      }

      car.offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
      consumers.add(car);
      return car;
    } catch (Exception e) {
      throw new FlumeException("Unable to connect to Kafka", e);
    }
  }

  private void migrateOffsets() {
    try (KafkaZkClient zkClient = KafkaZkClient.apply(zookeeperConnect,
            JaasUtils.isZkSaslEnabled(), ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, 10,
            Time.SYSTEM, "kafka.server", "SessionExpireListener", null, null);
         KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      Map<String, List<PartitionInfo>> topics = getKafkaTopics(consumer);
      for (String topic : topics.keySet()) {
        Map<TopicPartition, OffsetAndMetadata> kafkaOffsets = getKafkaOffsets(consumer, topic);
        if (!kafkaOffsets.isEmpty()) {
          logger.info("Found Kafka offsets for topic {}. Will not migrate from zookeeper", topic);
          logger.debug("Offsets found: {}", kafkaOffsets);
          continue;
        }

        logger.info("No Kafka offsets found. Migrating zookeeper offsets");
        Map<TopicPartition, OffsetAndMetadata> zookeeperOffsets =
                getZookeeperOffsets(zkClient, consumer, topic);
        if (zookeeperOffsets.isEmpty()) {
          logger.warn("No offsets to migrate found in Zookeeper");
          continue;
        }

        logger.info("Committing Zookeeper offsets to Kafka");
        logger.debug("Offsets to commit: {}", zookeeperOffsets);
        consumer.commitSync(zookeeperOffsets);
        // Read the offsets to verify they were committed
        Map<TopicPartition, OffsetAndMetadata> newKafkaOffsets = getKafkaOffsets(consumer, topic);
        logger.debug("Offsets committed: {}", newKafkaOffsets);
        if (!newKafkaOffsets.keySet().containsAll(zookeeperOffsets.keySet())) {
          throw new FlumeException("Offsets could not be committed");
        }
      }
    }
  }

  private Map<String, List<PartitionInfo>> getKafkaTopics(KafkaConsumer<String, byte[]> client) {
    Map<String, List<PartitionInfo>> allTopics = client.listTopics();

    Map<String, List<PartitionInfo>> filteredTopics = new HashMap<>();
    for (Map.Entry<String, List<PartitionInfo>> topicInfo : allTopics.entrySet()) {
      String topic = topicInfo.getKey();
      if ((topicsPattern.get() != null && topicsPattern.get().matcher(topic).matches())
              || topics.get().contains(topic)) {
        filteredTopics.put(topic, topicInfo.getValue());
      }
    }

    return filteredTopics;
  }

  private Map<TopicPartition, OffsetAndMetadata> getKafkaOffsets(
      KafkaConsumer<String, byte[]> client, String topic) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    List<PartitionInfo> partitions = client.partitionsFor(topic);
    for (PartitionInfo partition : partitions) {
      TopicPartition key = new TopicPartition(topic, partition.partition());
      OffsetAndMetadata offsetAndMetadata = client.committed(key);
      if (offsetAndMetadata != null) {
        offsets.put(key, offsetAndMetadata);
      }
    }
    return offsets;
  }

  private Map<TopicPartition, OffsetAndMetadata> getZookeeperOffsets(
          KafkaZkClient zkClient, KafkaConsumer<String, byte[]> consumer, String topic) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    List<PartitionInfo> partitions = consumer.partitionsFor(topic);
    for (PartitionInfo partition : partitions) {
      TopicPartition topicPartition = new TopicPartition(topic, partition.partition());
      Option<Object> optionOffset = zkClient.getConsumerOffset(groupId, topicPartition);
      if (optionOffset.nonEmpty()) {
        Long offset = (Long) optionOffset.get();
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
        offsets.put(topicPartition, offsetAndMetadata);
      }
    }
    return offsets;
  }

  private void decommissionConsumerAndRecords(ConsumerAndRecords c) {
    c.consumer.wakeup();
    c.consumer.close();
  }

  @VisibleForTesting
  void registerThread() {
    try {
      consumerAndRecords.get();
    } catch (Exception e) {
      logger.error(e.getMessage());
      e.printStackTrace();
    }
  }

  private enum TransactionType {
    PUT,
    TAKE,
    NONE
  }

  private class KafkaTransaction extends BasicTransactionSemantics {

    private TransactionType type = TransactionType.NONE;
    private Optional<ByteArrayOutputStream> tempOutStream = Optional
            .absent();
    // For put transactions, serialize the events and hold them until the commit goes is requested.
    private Optional<LinkedList<ProducerRecord<String, byte[]>>> producerRecords =
        Optional.absent();
    // For take transactions, hold records till commit goes through
    private Optional<LinkedList<ConsumerRecord<String, byte[]>>> consumerRecords
            = Optional.absent();
    private Optional<SpecificDatumWriter<AvroFlumeEvent>> writer =
            Optional.absent();
    private Optional<SpecificDatumReader<AvroFlumeEvent>> reader =
            Optional.absent();
    private Optional<LinkedList<Future<RecordMetadata>>> kafkaFutures =
            Optional.absent();
    private final String batchUUID = UUID.randomUUID().toString();

    // Fine to use null for initial value, Avro will create new ones if this
    // is null
    private BinaryEncoder encoder = null;
    private BinaryDecoder decoder = null;
    private boolean eventTaken = false;

    @Override
    protected void doBegin() throws InterruptedException {
      consumerAndRecords.get().rebalanceLock.lock();
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
      type = TransactionType.PUT;
      if (!producerRecords.isPresent()) {
        producerRecords = Optional.of(new LinkedList<ProducerRecord<String, byte[]>>());
      }
      String key = event.getHeaders().get(KEY_HEADER);

      Integer partitionId = null;
      try {
        if (staticPartitionId != null) {
          partitionId = staticPartitionId;
        }
        // Allow a specified header to override a static ID
        if (partitionHeader != null) {
          String headerVal = event.getHeaders().get(partitionHeader);
          if (headerVal != null) {
            partitionId = Integer.parseInt(headerVal);
          }
        }

        String topicToPublish = topics.get().get(0);
        if (partitionId != null) {
          producerRecords.get().add(
              new ProducerRecord<String, byte[]>(topicToPublish, partitionId, key,
                                                 serializeValue(event, parseAsFlumeEvent)));
        } else {
          producerRecords.get().add(
              new ProducerRecord<String, byte[]>(topicToPublish, key,
                                                 serializeValue(event, parseAsFlumeEvent)));
        }
        counter.incrementEventPutAttemptCount();
      } catch (NumberFormatException e) {
        throw new ChannelException("Non integer partition id specified", e);
      } catch (Exception e) {
        throw new ChannelException("Error while serializing event", e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Event doTake() throws InterruptedException {
      logger.trace("Starting event take");
      type = TransactionType.TAKE;
      try {
        if (!(consumerAndRecords.get().uuid.equals(channelUUID))) {
          logger.info("UUID mismatch, creating new consumer");
          decommissionConsumerAndRecords(consumerAndRecords.get());
          consumerAndRecords.remove();
        }
      } catch (Exception ex) {
        logger.warn("Error while shutting down consumer", ex);
      }
      if (!consumerRecords.isPresent()) {
        consumerRecords = Optional.of(new LinkedList<ConsumerRecord<String, byte[]>>());
      }
      ConsumerRecord<String, byte[]> record;

      try {
        if (!consumerAndRecords.get().failedRecords.isEmpty()) {
          record = consumerAndRecords.get().failedRecords.removeFirst();
        } else {
          if (logger.isTraceEnabled()) {
            logger.trace("Assignment during take: {}",
                    consumerAndRecords.get().consumer.assignment().toString());
          }

          long startTime = System.nanoTime();
          if (!consumerAndRecords.get().recordIterator.hasNext()) {
            consumerAndRecords.get().poll();
          }
          if (consumerAndRecords.get().recordIterator.hasNext()) {
            record = consumerAndRecords.get().recordIterator.next();
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1, batchUUID);
            consumerAndRecords.get().saveOffsets(tp, oam);

            long endTime = System.nanoTime();
            counter.addToKafkaEventGetTimer((endTime - startTime) / (1000 * 1000));

            if (logger.isDebugEnabled()) {
              logger.debug("{} processed output from partition {} offset {}",
                      new Object[]{getName(), record.partition(), record.offset()});
            }
          } else {
            return null;
          }
          counter.incrementEventTakeAttemptCount();
        }
        eventTaken = true;
        consumerRecords.get().add(record);

        Event e = deserializeValue(record.value(), parseAsFlumeEvent);

        //Add the key to the header
        if (record.key() != null) {
          e.getHeaders().put(KEY_HEADER, record.key());
        }

        return e;
      } catch (Exception ex) {
        logger.warn("Error while getting events from Kafka. This is usually caused by " +
                "trying to read a non-flume event. Ensure the setting for " +
                "parseAsFlumeEvent is correct", ex);
        throw new ChannelException("Error while getting events from Kafka", ex);
      }
    }

    @Override
    protected void doCommit() throws InterruptedException {
      try {
        logger.trace("Starting commit");
        if (type.equals(TransactionType.NONE)) {
          return;
        }
        if (type.equals(TransactionType.PUT)) {
          if (!kafkaFutures.isPresent()) {
            kafkaFutures = Optional.of(new LinkedList<Future<RecordMetadata>>());
          }
          try {
            long batchSize = producerRecords.get().size();
            long startTime = System.nanoTime();
            int index = 0;
            for (ProducerRecord<String, byte[]> record : producerRecords.get()) {
              index++;
              kafkaFutures.get().add(producer.send(record, new ChannelCallback(index, startTime)));
            }
            //prevents linger.ms from being a problem
            producer.flush();

            for (Future<RecordMetadata> future : kafkaFutures.get()) {
              future.get();
            }
            long endTime = System.nanoTime();
            counter.addToKafkaEventSendTimer((endTime - startTime) / (1000 * 1000));
            counter.addToEventPutSuccessCount(batchSize);
            producerRecords.get().clear();
            kafkaFutures.get().clear();
          } catch (Exception ex) {
            logger.warn("Sending events to Kafka failed", ex);
            throw new ChannelException("Commit failed as send to Kafka failed",
                    ex);
          }
        } else {
          // event taken ensures that we have collected events in this transaction
          // before committing
          if (consumerAndRecords.get().failedRecords.isEmpty() && eventTaken) {
            logger.trace("About to commit batch");
            long startTime = System.nanoTime();
            consumerAndRecords.get().commitOffsets();
            long endTime = System.nanoTime();
            counter.addToKafkaCommitTimer((endTime - startTime) / (1000 * 1000));
            if (logger.isDebugEnabled()) {
              logger.debug(consumerAndRecords.get().getCommittedOffsetsString());
            }
          }

          int takes = consumerRecords.get().size();
          if (takes > 0) {
            counter.addToEventTakeSuccessCount(takes);
            consumerRecords.get().clear();
          }
        }
      } finally {
        consumerAndRecords.get().rebalanceLock.unlock();
      }
    }

    @Override
    protected void doRollback() throws InterruptedException {
      try {
        if (type.equals(TransactionType.NONE)) {
          return;
        }
        if (type.equals(TransactionType.PUT)) {
          producerRecords.get().clear();
          kafkaFutures.get().clear();
        } else {
          counter.addToRollbackCounter(consumerRecords.get().size());
          consumerAndRecords.get().failedRecords.addAll(consumerRecords.get());
          consumerRecords.get().clear();
        }
      } finally {
        consumerAndRecords.get().rebalanceLock.unlock();
      }
    }

    private byte[] serializeValue(Event event, boolean parseAsFlumeEvent) throws IOException {
      byte[] bytes;
      if (parseAsFlumeEvent) {
        if (!tempOutStream.isPresent()) {
          tempOutStream = Optional.of(new ByteArrayOutputStream());
        }
        if (!writer.isPresent()) {
          writer = Optional.of(new
                  SpecificDatumWriter<AvroFlumeEvent>(AvroFlumeEvent.class));
        }
        tempOutStream.get().reset();
        AvroFlumeEvent e = new AvroFlumeEvent(
                toCharSeqMap(event.getHeaders()),
                ByteBuffer.wrap(event.getBody()));
        encoder = EncoderFactory.get()
                .directBinaryEncoder(tempOutStream.get(), encoder);
        writer.get().write(e, encoder);
        encoder.flush();
        bytes = tempOutStream.get().toByteArray();
      } else {
        bytes = event.getBody();
      }
      return bytes;
    }

    private Event deserializeValue(byte[] value, boolean parseAsFlumeEvent) throws IOException {
      Event e;
      if (parseAsFlumeEvent) {
        ByteArrayInputStream in =
                new ByteArrayInputStream(value);
        decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);
        if (!reader.isPresent()) {
          reader = Optional.of(
                  new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class));
        }
        AvroFlumeEvent event = reader.get().read(null, decoder);
        e = EventBuilder.withBody(event.getBody().array(),
                toStringMap(event.getHeaders()));
      } else {
        e = EventBuilder.withBody(value, Collections.EMPTY_MAP);
      }
      return e;
    }
  }

  /**
   * Helper function to convert a map of String to a map of CharSequence.
   */
  private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
    Map<CharSequence, CharSequence> charSeqMap =
            new HashMap<CharSequence, CharSequence>();
    for (Map.Entry<String, String> entry : stringMap.entrySet()) {
      charSeqMap.put(entry.getKey(), entry.getValue());
    }
    return charSeqMap;
  }

  /**
   * Helper function to convert a map of CharSequence to a map of String.
   */
  private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap = new HashMap<String, String>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }

  /* Object to store our consumer */
  class ConsumerAndRecords {
    final KafkaConsumer<String, byte[]> consumer;
    final String uuid;
    final LinkedList<ConsumerRecord<String, byte[]>> failedRecords
            = new LinkedList<ConsumerRecord<String, byte[]>>();
    final Lock rebalanceLock;

    ConsumerRecords<String, byte[]> records;
    volatile Iterator<ConsumerRecord<String, byte[]>> recordIterator;
    Map<TopicPartition, OffsetAndMetadata> offsets;

    ConsumerAndRecords(KafkaConsumer<String, byte[]> consumer, String uuid, Lock rebalanceLock) {
      this.consumer = consumer;
      this.uuid = uuid;
      this.records = ConsumerRecords.empty();
      this.recordIterator = records.iterator();
      this.rebalanceLock = rebalanceLock;
    }

    private void poll() {
      logger.trace("Polling with timeout: {}ms channel-{}", pollTimeout, getName());
      try {
        records = consumer.poll(Duration.ofMillis(pollTimeout).toMillis());
        recordIterator = records.iterator();
        logger.debug("{} returned {} records from last poll", getName(), records.count());
      } catch (WakeupException e) {
        logger.trace("Consumer woken up for channel {}.", getName());
      }
    }

    private void commitOffsets() {
      try {
        consumer.commitSync(offsets);
      } catch (Exception e) {
        logger.info("Error committing offsets.", e);
      } finally {
        logger.trace("About to clear offsets map.");
        offsets.clear();
      }
    }

    protected void revokePartitions(Collection<TopicPartition> partitions) {
      // `recordsIterator` might still contain records at this point.
      // That's, for example, because of different batch size on Sink side,
      // which defines TX boundaries
      // and batch size used for Kafka consumer.
      // And those remaining records might belong to revoked partitions.
      // So they have to be filtered out.
      // That's because at this point TX (and correspondingly offsets) have to be committed.
      // So consumer (for which partition is(or going to be) assigned)
      // will (re-)read them causing having
      // duplicates.
      Set<TopicPartition> revokedPartititons = new HashSet<>(partitions);

      if (!failedRecords.isEmpty()) {
        Iterator<ConsumerRecord<String, byte[]>> it = failedRecords.iterator();

        while (it.hasNext()) {
          ConsumerRecord<String, byte[]> rec = it.next();
          TopicPartition recTp = new TopicPartition(rec.topic(), rec.partition());

          if (revokedPartititons.contains(recTp)) {
            it.remove();
            logger.debug("Revoked record (being previoulsy fetched and failed) " +
                    "for topic {} - partition {} ", rec.topic(), rec.partition());
          }
        }
      }

      if (recordIterator.hasNext()) {
        List<ConsumerRecord<String, byte[]>> filteredRecords = new ArrayList<>();

        while (recordIterator.hasNext()) {
          ConsumerRecord<String, byte[]> rec = recordIterator.next();

          TopicPartition recTp = new TopicPartition(rec.topic(), rec.partition());
          if (!revokedPartititons.contains(recTp)) {
            filteredRecords.add(rec);
          } else {
            logger.debug("Revoked record (being previoulsy fetched) for topic {} - partition {} ",
                    rec.topic(), rec.partition());
          }
        }

        recordIterator = filteredRecords.iterator();
      }
    }

    private String getOffsetMapString() {
      StringBuilder sb = new StringBuilder();
      sb.append(getName()).append(" current offsets map: ");
      for (TopicPartition tp : offsets.keySet()) {
        sb.append("p").append(tp.partition()).append("-")
            .append(offsets.get(tp).offset()).append(" ");
      }
      return sb.toString();
    }

    // This prints the current committed offsets when debug is enabled
    private String getCommittedOffsetsString() {
      StringBuilder sb = new StringBuilder();
      sb.append(getName()).append(" committed: ");
      for (TopicPartition tp : consumer.assignment()) {
        try {
          sb.append("[").append(tp).append(",")
                  .append(consumer.committed(tp).offset())
                  .append("] ");
        } catch (NullPointerException npe) {
          logger.debug("Committed {}", tp);
        } catch (UnknownTopicOrPartitionException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("Committed {}", tp);
          }
        }
      }
      return sb.toString();
    }

    private void saveOffsets(TopicPartition tp, OffsetAndMetadata oam) {
      offsets.put(tp,oam);
      if (logger.isTraceEnabled()) {
        logger.trace(getOffsetMapString());
      }
    }
  }
}

// Throw exception if there is an error
class ChannelCallback implements Callback {
  private static final Logger log = LoggerFactory.getLogger(ChannelCallback.class);
  private int index;
  private long startTime;

  public ChannelCallback(int index, long startTime) {
    this.index = index;
    this.startTime = startTime;
  }

  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      log.trace("Error sending message to Kafka due to " + exception.getMessage());
    }
    if (log.isDebugEnabled()) {
      long batchElapsedTime = System.currentTimeMillis() - startTime;
      if (metadata != null) {
        log.debug("Acked message_no " + index + ": " + metadata.topic() + "-" +
                metadata.partition() + "-" + metadata.offset() + "-" + batchElapsedTime);
      }
    }
  }
}

class ChannelRebalanceListener implements ConsumerRebalanceListener {
  private static final Logger log = LoggerFactory.getLogger(ChannelRebalanceListener.class);
  private ConsumerAndRecords car;

  public ChannelRebalanceListener(ConsumerAndRecords car) {
    this.car = car;
  }

  // Set a flag that a rebalance has occurred. Then we can commit the currently written transactions
  // on the next doTake() pass.
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) {
      return;
    }

    log.info("waiting for \"in progress\" transaction to complete...");

    // Acquiring this lock means that "in progress" transaction completed.
    // And performing "revoking" with that lock being help is safety from consistency standpoint.
    car.rebalanceLock.lock();
    try {
      log.info("\"in progress\" transaction completed, proceeding with actual revoking partitions");

      car.revokePartitions(partitions);

      for (TopicPartition partition : partitions) {
        log.info("topic {} - partition {} revoked.", partition.topic(), partition.partition());
      }
    } finally {
      log.info("partitions revoking completed.", partitions);
      car.rebalanceLock.unlock();
    }
  }

  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    for (TopicPartition partition : partitions) {
      log.info("topic {} - partition {} assigned.", partition.topic(), partition.partition());
    }
  }
}
