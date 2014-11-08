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
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import redis.clients.jedis.Jedis;

/**
 *
 * @author VedaShree
 */
public class DataSaver extends BaseBasicBolt {

    Map<String, Integer> hashtags = new HashMap<String, Integer>();
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/test";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "28031981";

    Connection conn = null;
    Statement stmt = null;

    static org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(DataSaver.class);

    @Override
    public void cleanup() {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String city = input.getStringByField("city");
            org.json.JSONObject jsonObject = new org.json.JSONObject(city);
            LOG.info("DataSaver>>>>>>>>>>>>>>>>>>>>>." + jsonObject.getString("name") + ">" + jsonObject.getJSONObject("main").getString("temp"));
            PreparedStatement preparedStatement = conn.prepareStatement("insert into weather_data values(default,?,?,?,?,?,?,?)");
            preparedStatement.setString(1, jsonObject.getString("name"));
            preparedStatement.setString(2, jsonObject.getString("dt"));
            preparedStatement.setString(3, jsonObject.getJSONObject("main").getString("temp"));
            preparedStatement.setDouble(4, jsonObject.getJSONObject("main").getDouble("temp_min"));
            preparedStatement.setDouble(5, jsonObject.getJSONObject("main").getDouble("temp_max"));
            preparedStatement.setDouble(6, jsonObject.getJSONObject("main").getDouble("humidity"));
            preparedStatement.setDouble(7, jsonObject.getJSONObject("main").getDouble("pressure"));

            preparedStatement.executeUpdate();

        } catch (JSONException ex) {
            Logger.getLogger(DataSaver.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(DataSaver.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            Class.forName("com.mysql.jdbc.Driver");

            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DataSaver.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(DataSaver.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
