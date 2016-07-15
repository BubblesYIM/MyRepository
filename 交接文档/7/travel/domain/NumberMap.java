package com.jje.bigdata.userProfile.travel.domain;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.jje.bigdata.util.SparkUtils;


/**
 * @author javise.yan
 *
 */
public class NumberMap implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private Map<String, BigDecimal> map = new HashMap<String, BigDecimal>();

	public NumberMap(String data){
		this.setMapValue(data);
	}


	public NumberMap(){
		super();
	}
	
	public NumberMap(String key, BigDecimal value){
		this.put(key, value);
	}

	public NumberMap(String key, int value) {
		this.put(key, new BigDecimal(value));
	}

	public void putAll(NumberMap intMap) {
		if (intMap != null && !intMap.isEmpty()) {
			for (String key : intMap.keySet()) {
				if (this.containsKey(key)) {
					BigDecimal num = this.get(key);
					this.put(key, num.add(intMap.get(key)));
				} else {
					this.put(key, intMap.get(key));
				}
			}
		} 
	}
	
	
	public BigDecimal put(String key, BigDecimal value) {
		return this.map.put(key, value);
	}
	
	public String getDefinedMapString(){
		StringBuilder result = new StringBuilder("");
		if (!this.map.isEmpty()) {
			result.append("|");
			for (Entry<String, BigDecimal> entry : this.map.entrySet()) {
				result.append(entry.getKey() + ":" + entry.getValue());
				result.append("|");
			}
		}
		return result.toString();
	}

	public String getMapString() throws JsonGenerationException, JsonMappingException, IOException {
		return this.getMapJsonString();
	}
	
	public String getMapKeyString() throws JsonGenerationException, JsonMappingException, IOException {
		return this.getMapKeyJsonString();
	}

	public void setMapValue(String data) {
		if (SparkUtils.isNotBlank(data)) {
			String[] columns = data.split("[|]");
			for (String column : columns) {
				if(StringUtils.isEmpty(column)){
					continue;
				}
				String[] rows = column.split(":");
				this.put(rows[0], new BigDecimal(rows[1]));
			}
		}
	}
	
	public boolean isEmpty(){
		return this.map.isEmpty();
	}
	
	public boolean containsKey(Object obj){
		return this.map.containsKey(obj);
	}
	
	public BigDecimal get(Object obj){
		return this.map.get(obj);
	}

	public BigDecimal remove(Object key) {
		return map.remove(key);
	}

	public Set<String> keySet() {
		return this.map.keySet();
	}
	
	public Set<Entry<String, BigDecimal>> entrySet(){
		return this.map.entrySet();
	}

	public Integer size() {
		return this.map.size();
	}

	@Override
	public String toString() {
		return "IntegerMap [map=" + map + "]";
	}
	
	
	@JsonIgnore
	public String getMapJsonString() throws JsonGenerationException, JsonMappingException, IOException {
//		if (this.isEmpty()) {
//			return null;
//		}
		ObjectMapper objectMapper = new ObjectMapper();
		return objectMapper.writeValueAsString(this.map);
	}

	@JsonIgnore
	public String getMapKeyJsonString() throws JsonGenerationException, JsonMappingException, IOException {
		if (this.isEmpty() || this.map.keySet().isEmpty()) {
			return null;
		}
		ObjectMapper objectMapper = new ObjectMapper();
		return objectMapper.writeValueAsString(this.map.keySet());
	}
	
}
