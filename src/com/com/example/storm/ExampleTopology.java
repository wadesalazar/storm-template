package com.example.storm;


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
		
		/*-------------------------------------------------------------------- 
	     * Kafka Spout configuration\
	     * adding comment from another computer
	     -------------------------------------------------------------------- * */
		String kafkaTopic = "ex_topic";
        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("mimas.saturn:2181,rhea.saturn:2181,titan.saturn:2181"),
                kafkaTopic, "/kafka_storm", "StormSpout");
        spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
        spoutConfig.startOffsetTime = System.currentTimeMillis();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        
        /*-------------------------------------------------------------------- 
	     * Hive Bolt Config
	     -------------------------------------------------------------------- * */        
		// Hive connection configuration
	    String metaStoreURI = "thrift://titan.saturn:9083";
	    String dbName = "default";
	    String tblName = "test3";
	    // Fields for possible partition
	    //String[] partNames = {"name"};
	    // Fields for possible column data
	    String[] colNames = {"value1"};
	    // Record Writer configuration
	    DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
	            .withColumnFields(new Fields(colNames));
	    //        .withPartitionFields(new Fields(partNames));

	    HiveOptions hiveOptions;
	    hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper)
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
	    		"rhea.saturn", 
	    		5672, 
	    		"test", 
	    		"test", 
	    		ConnectionFactory.DEFAULT_VHOST, 
	    		10); 
	    // host, port, username, password, virtualHost, heartBeat 
	    ConsumerConfig spoutConfig1 = new ConsumerConfigBuilder().connection(connectionConfig)
	                                                            .queue("test_queue")
	                                                            .prefetch(200)
	                                                            .requeueOnFail()
	                                                            .build();
	    
	   /*-------------------------------------------------------------------- 
	    * Topology Builder
	    -------------------------------------------------------------------- * */
		
		TopologyBuilder builder = new TopologyBuilder();

		//builder.setSpout("name", new ExampleSpout(), 1);
		builder.setSpout("kafka-spout", kafkaSpout);
		
		builder.setSpout("rmq-spout", rabbitmq_spout)
	       .addConfigurations(spoutConfig1.asMap())
	       .setMaxSpoutPending(200);
		
		builder.setBolt("prep-bolt", new PrepBolt()).shuffleGrouping("kafka-spout").shuffleGrouping("rmq-spout");
		builder.setBolt("store-bolt", new HiveBolt(hiveOptions)).shuffleGrouping("prep-bolt");

		Config conf = new Config();
		conf.setDebug(true);

		conf.setNumWorkers(1);
		StormSubmitter.submitTopologyWithProgressBar("ExampleTopo", conf, builder.createTopology());

	}
	
	public static class PrepBolt extends BaseBasicBolt{

		private static final long serialVersionUID = 1L;
		
		public void execute(Tuple input, BasicOutputCollector collector) {
			String payload = "";
			if(input.getSourceComponent().equals("kafka-spout")){
				payload = new String((byte[]) input.getValue(0));
			}else if(input.getSourceComponent().equals("rmq-spout")){
				payload = input.getString(0);
			}else{
				payload = "unknown message from spout: " + input.getSourceComponent();
			}
			collector.emit(new Values(payload));
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("value1"));
		}
	}

}