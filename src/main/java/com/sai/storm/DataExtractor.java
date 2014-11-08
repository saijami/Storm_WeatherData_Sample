/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sai.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author VedaShree
 */
public class DataExtractor extends BaseBasicBolt{
	private static final long serialVersionUID = -3025639777071957758L;
	static Logger LOG = Logger.getLogger(DataExtractor.class);
	static JSONParser jsonParser = new JSONParser();
	
	@Override
	public void cleanup() {
	}
 
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String json = (String)input.getValueByField("tweet");
		try {
			org.json.JSONObject jsonObject = new org.json.JSONObject(json);
			
                        org.json.JSONArray cities = jsonObject.getJSONArray("list");
                        for(int i=0; i < cities.length(); i++) {
                            collector.emit(new Values("city",cities.getJSONObject(i).toString()));
                        }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("trackList","city"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

}