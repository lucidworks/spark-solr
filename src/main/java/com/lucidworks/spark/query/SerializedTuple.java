package com.lucidworks.spark.query;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.io.Tuple;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SerializedTuple implements Serializable {

	public SerializedTuple(Tuple tuple) {
		fields = tuple.getMap();
		this.EOF = tuple.EOF;
	}

	public SerializedTuple(HashMap m, boolean EOF) {
		fields = m;
		this.EOF = EOF;
	}

	private static final long serialVersionUID = 1L;

	public boolean EOF;
	public Map fields = new HashMap();

	public Object get(Object key) {
		return fields.get(key);
	}

	public void put(Object key, Object value) {
		fields.put(key, value);
	}

	public String getString(Object key) {
		return (String) fields.get(key);
	}

	public Long getLong(Object key) {
		return (Long) fields.get(key);
	}

	public Double getDouble(Object key) {
		return (Double) fields.get(key);
	}

	public List<String> getStrings(Object key) {
		return (List<String>) fields.get(key);
	}

	public List<Long> getLongs(Object key) {
		return (List<Long>) fields.get(key);
	}

	public List<Double> getDoubles(Object key) {
		return (List<Double>) fields.get(key);
	}

	public Map getMap() {
		return fields;
	}

	public List<Map> getMaps() {
		return (List<Map>) fields.get("_MAPS_");
	}

	public void setMaps(List<Map> maps) {
		fields.put("_MAPS_", maps);
	}

	public Map<String, Map> getMetrics() {
		return (Map<String, Map>) fields.get("_METRICS_");
	}

	public void setMetrics(Map<String, Map> metrics) {
		fields.put("_METRICS_", metrics);
	}

	public SerializedTuple clone() {
		return new SerializedTuple(new HashMap(fields), EOF);
	}
}