package com.example.kafka_test;

import lombok.var;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.*;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import org.apache.kafka.common.Node;

@Component
public class KafkaUtils {
    private static final Log log = LogFactory.getLog(KafkaUtils.class);

    public static final String BOOTSTRAP_SERVERS_PROP = "kafka.bootstrap.servers";

    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    protected final AdminClient adminClient;






    /**
     * to get the running port "kafka.bootstrap.servers"
     */

    public static String getBootstrapServers() {

        String bootstrapServers = System.getProperty(BOOTSTRAP_SERVERS_PROP, DEFAULT_BOOTSTRAP_SERVERS);
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        }
        return bootstrapServers;
    }

    public static Properties getDefaultAdminProperties() {
        Properties ret = new Properties();
        ret.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        return ret;
    }
    public KafkaUtils() {
        this(getDefaultAdminProperties());
    }

    /**
     * Public constructor.
     *
     * @param adminProperties
     *            an instance of {@link Properties}.
     */

    public KafkaUtils(Properties adminProperties) {
        this.adminClient = AdminClient.create(adminProperties);
    }


    /**
     * Check if the topic exist
     * @param {String} The name of the topic.
     * @return boolean
     */
    public boolean topicExists(String topic) {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult topics = this.adminClient.listTopics(options);
        boolean isExist=false;
        try {
            Set<String> currentTopicList = topics.names().get();
            isExist= currentTopicList.stream().anyMatch(t->t.contains(topic));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return isExist;
    }



    /**
     * create topic without replication
     * @param {String} The name of the topic.
     * @param {Integer} number of partitions.
     */

    public void createTopicWithoutReplication(String topic, int partitions) {
        createTopic(topic, partitions, (short) 1);
    }
    /**
     * create topic
     * @param {String} The name of the topic.
     * @param {Integer} number of partitions.
     * @param {Short} number of replications.
     */

    public void createTopic(String topic, int partitions, short replicationFactor) {
        log.info("Creating topic: " + topic + ", partitions: " + partitions + ", replications: " + replicationFactor);
        if (topicExists(topic)) {
            throw new IllegalArgumentException("Cannot create Topic already exists: " + topic);
        }
        CreateTopicsResult ret = adminClient.createTopics
                (Collections.unmodifiableList(Arrays.asList(new NewTopic(topic, partitions, replicationFactor))));

        try {
            ret.all().get();
        } catch ( InterruptedException | ExecutionException e ) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * list the topics
     * @return set<string>
     */

    public Set<String> listTopics() {
        try {
            return adminClient.listTopics().names().get();
        }catch ( InterruptedException | ExecutionException e ) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Description of a specific topic.
     * @param {String} The name of the topic.
     * @return TopicDescription
     */

    TopicDescription topicDescription(String topicName) {
        DescribeTopicsResult dt = adminClient.describeTopics(listTopics());
        try {
            return dt.values().get(topicName).get();
        } catch ( InterruptedException | ExecutionException e ) {
            throw new IllegalStateException(e);
        }
    }



    /**
     * Description of all topics.
     */

    Map allTopicsDescription() {

        DescribeTopicsResult dt = adminClient.describeTopics(listTopics());
        try {
            dt.all().get();

            return dt.all().get();
        } catch ( InterruptedException | ExecutionException e ) {
            throw new IllegalStateException(e);
        }
    }


  /**
     * List all consumers group.
     * @return List<String>
     */
    public List<ConsumerGroupListing> listAllConsumers() {
        long now = System.currentTimeMillis();
        try {
             return adminClient.listConsumerGroups()
                    .all()
                    .get()
                     .stream()
                     .collect(Collectors.toList());
        }catch ( InterruptedException | ExecutionException e ) {
            throw new IllegalStateException(e);
        }finally {
            this.adminClient.close();
        }
    }

    /**
     * get number of partitions in a specific topic.
     * @param {String}The name of the topic.
     * @return int
     */
    public int getNumberOfPartitions(String topic) {
        DescribeTopicsResult descriptions = adminClient.describeTopics(Collections.singletonList(topic));
        try {
            return descriptions.values().get(topic).get().partitions().size();
        } catch ( InterruptedException | ExecutionException e ) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * check if the groupconsummer still active.
     * @param {String} The groupid.
     * @return Boolean
     */

    public Boolean activeConsumers(final String groupId) throws ExecutionException, InterruptedException {
        boolean isExist=false;
        final DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(Arrays.asList(groupId),
                (new DescribeConsumerGroupsOptions()).timeoutMs(10 * 1000));
        final List<MemberDescription> members =
                new ArrayList<MemberDescription>(describeResult.describedGroups().get(groupId).get().members());
        if (!members.isEmpty()) {
            isExist=true;
            throw new IllegalStateException("Consumer group '" + groupId + "' is still active "
                    + "and has following members: " + members + ". "
                    + "Make sure to stop all running application instances before running the reset tool.");
        }
        return isExist;
    }

    /**
     * getLastestOffset of specific topic.
     * getLastestOffset of specific topic.
     */
    public static long getLatestOffset(KafkaConsumer consumer, String topic, int partitionId) {
        TopicPartition topicPartition = new TopicPartition(topic, partitionId);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToEnd(Arrays.asList(topicPartition));
        return consumer.position(topicPartition);
    }
    /**
     * get earliest offset of specific topic
     */
    public static long getEarliestOffset(KafkaConsumer consumer, String topic, int partitionId) {
        TopicPartition topicPartition = new TopicPartition(topic, partitionId);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToBeginning(Arrays.asList(topicPartition));
        return consumer.position(topicPartition);
    }

    /**
     * Modify configuration values for a specific topic.
     * @param topic The topic to modify.
     * @param configItems Map of Key to Value to modify.
     * @return boolean
     */
    public void alterTopicConfig(final String topic, final Map<String, String> configItems) {
        try {
            // Define the resource we want to modify, the topic.
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            final List<ConfigEntry> configEntries = new ArrayList<>();
            for (final Map.Entry<String, String> entry : configItems.entrySet()) {
                configEntries.add(
                        new ConfigEntry(entry.getKey(), entry.getValue())
                );
            }
            // Define the configuration set
            final Config config = new Config(configEntries);
            // Create the topic
            final AlterConfigsResult result = adminClient   .alterConfigs(Collections.singletonMap(configResource, config));
            // Wait for the async request to process.
            result.all().get();

        } catch ( InterruptedException | ExecutionException e ) {
            throw new IllegalStateException(e);
        }

    }


    public CreatePartitionsResult addPartitionsForTopic(String topic, int partitions) {
        Map<String, NewPartitions> map = new HashMap<>();
        NewPartitions np = NewPartitions.increaseTo(partitions);
        map.put(topic, np);
        return adminClient.createPartitions(map);
    }


    /**
     * Get the configuration for topic.
     * ????????????????????????????
     */



    /**
     * Remove a topic from a kafka cluster.  This is destructive!
     * @param topic name of the topic to remove.
     * @return true on success.
     */
    public void deleteTopic(final String topic) {
        try {
            final DeleteTopicsResult result = adminClient.deleteTopics(Collections.singletonList(topic));

            // Wait for the async request to process.
            result.all().get();


        } catch ( InterruptedException | ExecutionException e ) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * Removes a consumer's state.
     * @param id id of consumer group to remove.
     * @return boolean true if success.
     * @throws RuntimeException on underlying errors.
     */
    public boolean removeConsumerGroup(final String id) {
        final DeleteConsumerGroupsResult request = adminClient.deleteConsumerGroups(Collections.singleton(id));
        try {
            request.all().get();
            return true;
        } catch ( InterruptedException | ExecutionException e ) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * Describe nodes within Kafka cluster.
     * @return Collection of nodes within the Kafka cluster.
     */
    public List<Node> describeClusterNodes() {
        try  {
            final DescribeClusterResult describeClusterResult = adminClient.describeCluster();
            return new ArrayList<>(describeClusterResult.nodes().get());
        } catch (final InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    public int numberOfBrokers() {
        try {
            return adminClient.describeCluster().nodes().get().size();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Unable to get number of brokers.", e);
        }
    }


    private Map<String, Object> buildDefaultConfig() {
        final Map<String, Object> defaultClientConfig = new HashMap<>();
        defaultClientConfig.put("bootstrap.servers","localhost:9092" );
        defaultClientConfig.put("client.id", "test-consumer-id");
        defaultClientConfig.put("request.timeout.ms", 15000);
        return defaultClientConfig;
    }


    /**
     * Create a kafka producer that is connected to local server.
     * @return KafkaProducer configured to produce into Test server.
     */
    public <K, V> KafkaProducer<K, V> getKafkaProducer(final Properties config) {
        // Build config
        final Map<String, Object> kafkaProducerConfig = buildDefaultConfig();
        //kafkaProducerConfig.put("bootstrap.servers",getBootstrapServers());
        kafkaProducerConfig.put("max.in.flight.requests.per.connection", 1);
        //kafkaProducerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        kafkaProducerConfig.put("retries", 5);
        kafkaProducerConfig.put("client.id", getClass().getSimpleName() + " Producer");
        kafkaProducerConfig.put("batch.size", 0);
        kafkaProducerConfig.put("key.serializer", StringSerializer.class);
        kafkaProducerConfig.put("value.serializer", StringSerializer.class);


        if (config != null) {
            for (final Map.Entry<Object, Object> entry: config.entrySet()) {
                kafkaProducerConfig.put(entry.getKey().toString(), entry.getValue());
            }
        }
        // Create and return Producer.
        return new KafkaProducer<>(kafkaProducerConfig);
    }


    /**
     * Produce into the defined kafka topic.
     * @param topicName the topic to produce into.
     * @param Value the message .
     * @return List of ProducedKafkaRecords.
     */
    public void produceRecords(
            final String topicName,
            final String Value
    ) {
        // Defines customized producer properties
        final Properties producerProperties = new Properties();
        producerProperties.put("acks", "all");

        final List<ProducerRecord> producedRecords = new ArrayList<>();
        try (final KafkaProducer producer = getKafkaProducer(
                producerProperties
        )) {
             final ProducerRecord record
                        = new ProducerRecord(topicName, Value);
             producedRecords.add(record);
              // Send it.
            producer.send(record);
            // Publish to the topic and close.
            producer.flush();

            }
            log.debug("Produce completed");
        }


    /**
     * Return Kafka Consumer configured to consume from internal Kafka Server.
     * @param config configuration options to be set.
     */
    public <K, V> KafkaConsumer<K, V> getKafkaConsumer(final Properties config) {
        // Build config
        final Map<String, Object> kafkaConsumerConfig = buildDefaultConfig();
        kafkaConsumerConfig.put("key.deserializer", StringDeserializer.class);
        kafkaConsumerConfig.put("value.deserializer", StringDeserializer.class);

        if (config != null) {
            for (final Map.Entry<Object, Object> entry: config.entrySet()) {
                kafkaConsumerConfig.put(entry.getKey().toString(), entry.getValue());
            }
        }

        // Create and return Consumer.
        return new KafkaConsumer<>(kafkaConsumerConfig);
    }


    /**
     * Consume all records from all partitions on the given topic.
     * @param topic Topic to consume from.
     * @return List of ConsumerRecords consumed.
     */
    public List<Record> consumeAllRecordsFromTopic(final String topic) {
        // Find all partitions on topic.
        final TopicDescription topicDescription = topicDescription(topic);
        final Collection<Integer> partitions = topicDescription
                .partitions()
                .stream()
                .map(TopicPartitionInfo::partition)
                .collect(Collectors.toList());

        // Consume messages
        var list = consumeAllRecordsFromTopic(
              topic  , partitions);
        var records = list.stream().map(x -> new Record(x.offset(), x.value().toString())).collect(Collectors.toList());
        return records;
    }

    /**
     * Consume all records from the partitions passed on the given topic.
     * @param topic Topic to consume from.
     * @param partitionIds Which partitions to consume from.
     * @return List of ConsumerRecords consumed.
     */
    public <K, V> List<ConsumerRecord<K, V>> consumeAllRecordsFromTopic(
            final String topic,
            final Collection<Integer> partitionIds
    ) {
        // Create topic Partitions
        final List<TopicPartition> topicPartitions = partitionIds
                .stream()
                .map((partitionId) -> new TopicPartition(topic, partitionId))
                .collect(Collectors.toList());

        // Holds our results.
        final List<ConsumerRecord<K, V>> allRecords = new ArrayList<>();

        // Connect Consumer
        try (final KafkaConsumer<K, V> kafkaConsumer = getKafkaConsumer( new Properties())) {

            // Assign topic partitions & seek to head of them
            kafkaConsumer.assign(topicPartitions);
            kafkaConsumer.seekToBeginning(topicPartitions);

            // Pull records from kafka, keep polling until we get nothing back
            ConsumerRecords<K, V> records;
            records = kafkaConsumer.poll(Duration.ofMillis(2000L));

                // Add to our array list
                records.forEach(allRecords::add);


        }


        return allRecords;
    }








}
