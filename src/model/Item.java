package model;

import java.util.Map;


public class Item {
	private Map<String, String> attributes;
	
	public Item(Map<String, String> attributes) {
		this.setAttributes(attributes);
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<String, String> attributes) {
		this.attributes = attributes;
	}
	
	public void addAttribute(String name, String value){
		attributes.put(name, value);
	}
}
