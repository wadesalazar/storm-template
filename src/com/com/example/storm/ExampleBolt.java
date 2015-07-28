package com.example.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;


public class ExampleBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -766938007500152933L;
	public void execute(Tuple tuple, BasicOutputCollector collector){
		System.out.println(tuple);
	}
	public void declareOutputFields(OutputFieldsDeclarer ofd){

	}
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}