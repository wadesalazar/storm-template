package com.example.storm;


import java.io.UnsupportedEncodingException;


import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import com.rabbitmq.client.ConnectionFactory;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import io.latent.storm.rabbitmq.RabbitMQSpout;
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfigBuilder;


public class ExampleTopology{


	public static void main(String[] args) throws Exception {
		
		//Kakfa
		String kafkaTopic = "ex_topic";
		String kafkaZkHosts = "mimas.saturn:2181,rhea.saturn:2181,titan.saturn:2181";
		//Hive
		String hivemetastorURI = "thrift://titan.saturn:9083";
	    String hiveDB = "default";
	    String hiveTable = "test3";
	    String[] hiveTableCols = {"value1"};
		//Storm
	    String stormTopic = "ExampleTopo";
		//RabbitMQ
		String rmqServer = "rhea.saturn";
	    String rmqServerPort = "5672";
	    String rmqUserID = "test";
	    String rmqUserPW = "test";
	    String rmqQueue = "test_queue";
		
		/*-------------------------------------------------------------------- 
	     * Kafka Spout configuration\
	     * adding comment from another computer
	     -------------------------------------------------------------------- * */
		// do we need to make the dir and name configurable?
        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts(kafkaZkHosts),
                kafkaTopic, "/kafka_storm", "StormSpout");
        spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
        spoutConfig.startOffsetTime = System.currentTimeMillis();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        
        /*-------------------------------------------------------------------- 
	     * Hive Bolt Config
	     -------------------------------------------------------------------- * */        
	    // Fields for possible partition
	    //String[] partNames = {"name"};
	    
	    // Record Writer configuration
	    DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
	            .withColumnFields(new Fields(hiveTableCols));
	    //        .withPartitionFields(new Fields(partNames));

	    HiveOptions hiveOptions;
	    hiveOptions = new HiveOptions(hivemetastorURI, hiveDB, hiveTable, mapper)
	            .withTxnsPerBatch(2)
	            .withBatchSize(10)
	            .withIdleTimeout(10);
	            //.withKerberosKeytab(path_to_keytab)
	            //.withKerberosPrincipal(krb_principal);
	    
	    /*-------------------------------------------------------------------- 
	     * Rabbit MQ Spout Config
	     -------------------------------------------------------------------- * */
	    Scheme scheme = new SimpleStringScheme();
	    IRichSpout rabbitmq_spout = new RabbitMQSpout(scheme);
	    
	    ConnectionConfig connectionConfig = new ConnectionConfig(
	    		rmqServer, 
	    		Integer.parseInt(rmqServerPort), 
	    		rmqUserID, 
	    		rmqUserPW, 
	    		ConnectionFactory.DEFAULT_VHOST, 
	    		10); 
	    // host, port, username, password, virtualHost, heartBeat 
	    ConsumerConfig spoutConfig1 = new ConsumerConfigBuilder().connection(connectionConfig)
	                                                            .queue(rmqQueue)
	                                                            .prefetch(200)
	                                                            .requeueOnFail()
	                                                            .build();
	    
	   /*-------------------------------------------------------------------- 
	    * Topology Builder
	    -------------------------------------------------------------------- * */
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("kafka-spout", kafkaSpout);
		
		builder.setSpout("rmq-spout", rabbitmq_spout)
	       .addConfigurations(spoutConfig1.asMap())
	       .setMaxSpoutPending(200);
		
		builder.setBolt("prep-bolt", new PrepBolt(hiveTableCols))
			.shuffleGrouping("kafka-spout")
			.shuffleGrouping("rmq-spout");
		builder.setBolt("store-bolt", new HiveBolt(hiveOptions))
			.shuffleGrouping("prep-bolt");

		Config conf = new Config();
		conf.setDebug(true);

		conf.setNumWorkers(1);
		StormSubmitter.submitTopologyWithProgressBar(stormTopic, conf, builder.createTopology());

	}
	
	public static class PrepBolt extends BaseBasicBolt{

		private static final long serialVersionUID = 1L;
		private Fields fields = null;		
		
		PrepBolt(String[] hiveTableCols){
			fields = new Fields(hiveTableCols);
		}
		
		public void execute(Tuple input, BasicOutputCollector collector) {
			//we should check that the number of expected fields match the number found in the tuple
			Fields inFields = input.getFields();
			Values values = null;
			
			String inputString = null;
			String[] inputArray = null;
					
			if(input.getSourceComponent().equals("kafka-spout")){
				
				values = new Values(input.getString(0));
			
			}else if(input.getSourceComponent().equals("rmq-spout")){

				try {
					inputString = new String((byte[]) input.getValueByField(inFields.get(0)), "UTF-8");
					inputArray = inputString.split(",");
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		       
				values = new Values(inputArray.toString());
				
			}else{
				//need error logging here
			}
			collector.emit(values);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// add comma delimited list of fields below
			declarer.declare(fields);
		}
	}

}