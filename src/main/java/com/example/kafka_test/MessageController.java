package com.example.kafka_test;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("api/v1/messages")
// @EnableKafka pour quil nous configure kafkatemplate
public class MessageController {

    private final KafkaUtils kafkautils;
    //injection a travers un constructeur
    private KafkaTemplate<String,String> kafkaTemplate;
    //KafkaTemplate<CLé, valeur>

    @Autowired
    public MessageController(KafkaTemplate<String, String> kafkaTemplate,KafkaUtils kafkautils) {
        this.kafkautils=kafkautils;
        this.kafkaTemplate = kafkaTemplate;
    }

    //envoyer un msg vers le topic test
    @PostMapping
    public void publish(@RequestBody String message){
     kafkaTemplate.send("test",message);
    }




    /**
     * topic treatment
     */


    /**
     *Allows to get a list of topics
     */
    @GetMapping(value = "/topics",produces = MediaType.APPLICATION_JSON_VALUE)
    public Set<String>  listOfTopics(){

        Set<String> topics=kafkautils.listTopics();
        System.out.println("liste des topics :"+topics);
        return topics;
    }



    /**
     *Allows to create a topic without replication
     * */
    @RequestMapping(value = "/createtopicWR", method = RequestMethod.POST)
    public ResponseEntity<Void> createTopicWR(@RequestParam("topic") String topicName,
                                              @RequestParam("partition") int partition){

        kafkautils.createTopicWithoutReplication(topicName, partition);
        return ResponseEntity.ok().build();

    }

    /**
     *Allows to create a topic with replication
     */
    @RequestMapping(value = "/createtopic", method = RequestMethod.POST)
    public ResponseEntity<Void> createTopic(@RequestParam("topic") String topicName,
                                            @RequestParam("partition") int partition,
                                            @RequestParam("replication") int replication){

        kafkautils.createTopic(topicName, partition,(short)replication);
        return ResponseEntity.ok().build();

    }

    /**
     *Allows to delete a topic
    */
    @DeleteMapping(value = "delete/{name}",produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> delete(@PathVariable String name){

        kafkautils.deleteTopic(name);
        return new ResponseEntity<>("Topic has been deleted successfully ", HttpStatus.ACCEPTED);
    }


    /**
     *Allows to describe a specific topic
     */
    @GetMapping(value = "/describeTopic/{topicname}",produces = MediaType.APPLICATION_JSON_VALUE)
    public  ResponseEntity<TopicDescription>  descriptionTopic(@PathVariable("topicname")String topicname){

        TopicDescription topic=kafkautils.topicDescription(topicname);
        System.out.println("description :"+topic);
        return new ResponseEntity<>(topic, HttpStatus.OK);

    }

    /**
     *Allows to describe all the topics
     */
    @GetMapping(value = "/describeAllTopic",produces = MediaType.APPLICATION_JSON_VALUE)
    public  ResponseEntity<String>  describeTopics(){

        Map topics=kafkautils.allTopicsDescription();
        System.out.println("topics description :"+topics);
        return new ResponseEntity<>(topics.toString(), HttpStatus.OK);

    }

    /**
     *Allows to add partitions for a particular topic
     */
    @RequestMapping(value = "/alterTopic", method = RequestMethod.POST)
    public ResponseEntity<String> alterTopic(@RequestParam("topic") String topic,
                                             @RequestParam("partitions")int partitions
    )  {
        kafkautils.addPartitionsForTopic(topic,partitions);
        return new ResponseEntity<>("alter topic configuration", HttpStatus.OK);

    }

    /**
     *Allows to get the last offset to specific partition
     */
    @RequestMapping(value = "/getLatestOffset", method = RequestMethod.GET)
    public ResponseEntity<Long> getLatestOffset(@RequestParam("groupId")KafkaConsumer groupId ,
                                                @RequestParam("topic")String topic,
                                                @RequestParam("partitionId")int partitionId)  {

        Long LatestOffset=kafkautils.getLatestOffset(groupId,topic,partitionId);
        return new ResponseEntity<>(LatestOffset, HttpStatus.OK);
    }

    /**
     *Allows to get the earliest offset to specific partition
     */
    @RequestMapping(value = "/getEarliestOffset", method = RequestMethod.GET)
    public ResponseEntity<Long> getEarliestOffset(@RequestParam("groupId")KafkaConsumer groupId ,
                                                  @RequestParam("topic")String topic,
                                                  @RequestParam("partitionId")int partitionId)  {

        Long LatestOffset=kafkautils.getEarliestOffset(groupId,topic,partitionId);
        return new ResponseEntity<>(LatestOffset, HttpStatus.OK);
    }

    /**
     * producers
     */

    /**
     *Allows to produce in a specific topic
     */
    @RequestMapping(value = "/produceRecords", method = RequestMethod.POST)
    public ResponseEntity<String> produceRecords(@RequestParam("topicName") String topicName,
                                                 @RequestParam("Value") String Value)  {
        kafkautils.produceRecords(topicName, Value);
        return new ResponseEntity<>( "send",HttpStatus.OK);
    }

    /**
     * consummers
     */

    /**
     *Allows to get the list of all consummers
     */

    @RequestMapping(value = "/listofconsummers", method = RequestMethod.GET)
    public ResponseEntity<List<ConsumerGroupListing>> listOfconsummers(){

        return new ResponseEntity<>(kafkautils.listAllConsumers(), HttpStatus.OK);
    }


    /**
     *Allows to check if the groupconsummer still active.
     */
    @RequestMapping(value = "/noActiveconsummers/{groupId}", method = RequestMethod.GET)
    public ResponseEntity<String> noActiveConsummers(@PathVariable("groupId")String groupId) throws ExecutionException, InterruptedException {

        Boolean active=kafkautils.activeConsumers(groupId);
        if(active)
        return new ResponseEntity<>("is still active", HttpStatus.OK);
        else
        return new ResponseEntity<>("is inactive", HttpStatus.OK);

    }


    /**
     *Allows to get all records from specific topic .
     */
    @RequestMapping(value = "/consumeAllRecordsFromTopic/{topicName}", method = RequestMethod.GET)
    public ResponseEntity<?> consumeAllRecordsFromTopic(@PathVariable("topicName")String topicName) throws JsonProcessingException {
        List<Record> res= kafkautils.consumeAllRecordsFromTopic(topicName);
        System.out.println("les messageeee"+res);
        //Creating the ObjectMapper object
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(res);
        return new ResponseEntity<String>(jsonString.toString(),HttpStatus.OK);
    }

    /**
     *Allows to remove consumer group.
     */
    @DeleteMapping(value = "removeConsumerGroup/{name}",produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> removeConsumerGroup(@PathVariable String name){

        kafkautils.removeConsumerGroup(name);
        return new ResponseEntity<>("consummergroup has been deleted successfully ", HttpStatus.ACCEPTED);
    }



    /**
     * clusters
     */

    /**
     *Allows to get all clusters.
     */
    @RequestMapping(value = "/describeClusterNodes", method = RequestMethod.GET)
    public ResponseEntity<List<Node>> describeClusterNodes()  {
        List<Node> description = kafkautils.describeClusterNodes();
        System.out.println("clusteeer"+description);
        return new ResponseEntity<List<Node>>(description, HttpStatus.OK);
    }





}
