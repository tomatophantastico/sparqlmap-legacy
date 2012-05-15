package org.aksw.sparqlmap.config.syntax;


public class ConstantValueColumn extends ColumDefinition {


	
	private String value;
	private boolean resource;
	
	
	public String getValue() {
		return value;
	}
	
	public void setValue(String value) {
		this.value = value;
	}
	
	public void setResource(boolean resource) {
		this.resource = resource;
	}
	
	public boolean isResource() {
		return resource;
	}
	
	


}
