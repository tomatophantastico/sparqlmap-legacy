package org.aksw.sparqlmap.config.syntax;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class NameSpaceConfig {
	

	private String schemaBase;
		
		private String instanceBase;
	
	
	private BiMap<String,String> namespaces = HashBiMap.create() ;
	
	public String getSchemaBase() {
		return schemaBase;
	}

	public void setSchemaBase(String schemaBase) {
		this.schemaBase = schemaBase;
	}

	public String getInstanceBase() {
		return instanceBase;
	}

	public void setInstanceBase(String instanceBase) {
		this.instanceBase = instanceBase;
	}
	
	
	public String  getURLForPrefix(String prefix){
		return  (String) namespaces.get(prefix);
	}
	
	public String getPrefixForUrl(String url){
		return (String) namespaces.inverse().get(url);
	}
	
	public void putNamespaceMapping(String prefix, String url){
		namespaces.put(prefix, url);
	}
	
	
		
	public String resolveToProperty(String resolve){
		
		//remove the dataype definition
		if(resolve.contains("^^")){
			resolve = resolve.substring(0,resolve.indexOf("^^"));
		}
	
		return resolveUri(resolve);
		
		
	}
	
	public Resource resolveToResource(String resolve){
		return ResourceFactory.createResource(resolveUri(resolve));
	}
	
	/**
	 * creates a string representation of an URL that should not collide with restriction in db use, especially aliases.
	 * @param url
	 * @return
	 */
	public static String escapeUrlForDBUse(String url){
		String escaped = url.replaceAll(":", "_");
		escaped = escaped.replaceAll("/", "_");
		escaped = escaped.replaceAll("#", "_");
		escaped = escaped.replaceAll("", "_");
		
		if(escaped.length()>200){
			escaped = escaped.substring(0,200) + 
				escaped.hashCode();
		}
			
		return escaped;	
	}
	
	

	
	
	
	
	
	public String resolveUri(String resolve){
		String fullUri = null;
		
		// TODO  better validation
		//may be not the best way.....
		if(resolve.equals("NULL")){
			fullUri = null; 
		}else if(resolve.contains("://")){
			fullUri =  resolve;
			
		}else{
			String[] resolveSplit = resolve.split(":");
			if(resolveSplit.length<2){
				//we use the default local name space
				throw new RuntimeException("You mus present uri or prefixed resource adresses until now");

			}
			fullUri =  namespaces.get(resolveSplit[0]) + resolveSplit[1];
		}
		
		
		return fullUri;
		
	}
	

}
