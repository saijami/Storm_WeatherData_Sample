package com.sai.storm;

/**
 *
 * @author VedaShree
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.logging.Level;
import org.json.JSONArray;

import org.json.JSONException;
import org.json.JSONObject;

public class ApiStreamingSpout extends BaseRichSpout implements Runnable {

    static String STREAMING_API_URL = "https://stream.twitter.com/1/statuses/filter.json?track=";
    private String track;
    private String user;
    private String password;
    private DefaultHttpClient client;
    private SpoutOutputCollector collector;
    private UsernamePasswordCredentials credentials;
    private BasicCredentialsProvider credentialProvider;
    static Logger LOG = Logger.getLogger(ApiStreamingSpout.class);
    //JSONArray cityList = new JSONArray();

    //LinkedBlockingQueue<String> feed = new LinkedBlockingQueue<String>();
    String feed;

    @Override
    public void nextTuple() {
        if (feed.length()> 0) {
                collector.emit(new Values(track, feed));
        }
        feed = "";
    }

    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }


    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        this.collector = collector;
        new Thread(this).start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("criteria", "tweet"));
    }

    @Override
    public void run() {
        /*
         * Create the client call
         */
        while (true) {
            InputStream is = null;
            try {
                feed = "";
                is = new URL("http://api.openweathermap.org/data/2.5/group?id=524901,703448,2643743,1271881&units=metric").openStream();
                BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
                String jsonText = readAll(rd);
                feed = ""+jsonText;
            } catch (MalformedURLException ex) {
                java.util.logging.Logger.getLogger(ApiStreamingSpout.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(ApiStreamingSpout.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    is.close();
                } catch (IOException ex) {
                    java.util.logging.Logger.getLogger(ApiStreamingSpout.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            try {
                Thread.sleep(1000*60*5);
            } catch (Exception e) {
            }
        }
    }
}
