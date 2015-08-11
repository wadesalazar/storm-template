package com.example.storm;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Charsets;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class DelimitedStringScheme implements Scheme {

	private static final long serialVersionUID = -5297327674006934264L;
	private String encoding = null;
	
	public DelimitedStringScheme(String inEncoding) {
		this.encoding = inEncoding;
	}
	
	public DelimitedStringScheme(){
		
		this(Charsets.UTF_8.toString());
	}
	
	
	/**
	 * 
	 */
	
	public List<Object> deserialize(byte[] ser) {
		String chars = null;
        List<String> items = null;
        try {
            chars = new String(ser, encoding);
            items = Arrays.asList(chars.split("\\s*,\\s*"));
            //need to ensure the number of provided fields matches header and error
        } catch (UnsupportedEncodingException e) {
            //logger.error("fail to deserialize");
        }

        return Collections.singletonList((Object)items);
	}

	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return null;
	}

}
