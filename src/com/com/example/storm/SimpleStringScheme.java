package com.example.storm;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Charsets;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class SimpleStringScheme implements Scheme {

    /**
     * 
     */
    private static final long serialVersionUID = 3195794623690787473L;
    
    //private static final Logger logger = LoggerFactory.getLogger(SimpleStringScheme.class);

    private final String encoding;

    public SimpleStringScheme(String inEncoding) {
        this.encoding = inEncoding;
    }

    public SimpleStringScheme() {
        this(Charsets.UTF_8.toString());
    }

    public List<Object> deserialize(byte[] ser) {
        String chars = null;

        try {
            chars = new String(ser, encoding);

        } catch (UnsupportedEncodingException e) {

        }

        return Collections.singletonList((Object)chars);
    }

    public Fields getOutputFields() {
    	//not sure how to return multiple fields here
        return new Fields("value1");
    }

}
