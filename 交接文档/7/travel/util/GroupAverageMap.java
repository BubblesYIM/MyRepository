package com.jje.bigdata.userProfile.travel.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.jje.bigdata.util.SparkUtils;

public class GroupAverageMap implements Serializable {
	private static final long serialVersionUID = 1L;
	private Map<String, AverageUnit> dataMap = new HashMap<String, AverageUnit>();

	public GroupAverageMap() {
		super();
	}

	public GroupAverageMap(String data) throws JsonParseException, JsonMappingException, IOException {
		this();
		this.setJsonValue(data);
	}

	public GroupAverageMap(String key, AverageUnit value) {
		this();
		this.put(key, value);
	}

	public void putAll(GroupAverageMap inMap) {
		if (inMap == null || inMap.isEmpty()) {
			return;
		}
		Set<Entry<String, AverageUnit>> inEntrySet = inMap.entrySet();
		for (Entry<String, AverageUnit> inEntry : inEntrySet) {
			if (!this.containsKey(inEntry.getKey())) {
				this.put(inEntry.getKey(), inEntry.getValue());
				continue;
			}
			AverageUnit thisData = this.get(inEntry.getKey());
			thisData.add(inEntry.getValue());
		}
	}

	public Map<String, AverageUnit> getDataMap() {
		return dataMap;
	}

	public AverageUnit put(String key, AverageUnit value) {
		return this.dataMap.put(key, value);
	}

	@JsonIgnore
	public boolean isEmpty() {
		return this.dataMap.isEmpty();
	}

	public boolean containsKey(Object obj) {
		return this.dataMap.containsKey(obj);
	}

	public AverageUnit get(Object obj) {
		return this.dataMap.get(obj);
	}

	public Set<String> keySet() {
		return this.dataMap.keySet();
	}

	public Set<Entry<String, AverageUnit>> entrySet() {
		return this.dataMap.entrySet();
	}

	public Integer size() {
		return this.dataMap.size();
	}

	@JsonIgnore
	public String getSampleString() {
		StringBuilder result = new StringBuilder("");
		if (!this.dataMap.isEmpty()) {
			result.append("|");
			for (Entry<String, AverageUnit> entry : this.dataMap.entrySet()) {
				result.append(entry.getKey() + ":" + entry.getValue() == null ? null : entry.getValue().getValue());
				result.append("|");
			}
		}
		return result.toString();
	}

	@JsonIgnore
	public String getJsonString() throws JsonGenerationException, JsonMappingException, IOException {
		if (this.isEmpty()) {
			return null;
		}
		ObjectMapper objectMapper = new ObjectMapper();
		return objectMapper.writeValueAsString(this.getDataMap());
	}

	public void setJsonValue(String data) throws JsonParseException, JsonMappingException, IOException {
		GroupAverageMap readValue = GroupAverageMap.newInstance(data);
		if (readValue == null) {
			return;
		}
		this.dataMap = readValue.getDataMap();
	}

	public static GroupAverageMap newInstance(String data) throws IOException, JsonParseException, JsonMappingException {
		if (SparkUtils.isNull(data)) {
			return null;
		}
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		GroupAverageMap readValue = objectMapper.readValue(data, GroupAverageMap.class);
		return readValue;
	}

	@Override
	public String toString() {
		try {
			return getJsonString();
		} catch (JsonGenerationException e) {
						e.printStackTrace();
		} catch (JsonMappingException e) {
						e.printStackTrace();
		} catch (IOException e) {
						e.printStackTrace();
		}
		return "GroupCalculateMap [map=" + dataMap + "]";
	}

}
