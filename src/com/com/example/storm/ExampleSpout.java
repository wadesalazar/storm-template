
package com.example.storm;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class ExampleSpout extends BaseRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5047947463940417783L;
	
	SpoutOutputCollector _collector;

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector){
		_collector = collector;
	}
	public void nextTuple(){
		Utils.sleep(2000);
		_collector.emit(new Values("wadeadamsalazar"));
	}

	@Override
	public void ack(Object id){

	}

	@Override
	public void fail(Object id){

	}
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("value1"));
	}
	
	
}