package org.aksw.sparqlmap.web.spring;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.web.context.ConfigurableWebApplicationContext;

public class WebAppContextInitializer implements ApplicationContextInitializer<ConfigurableWebApplicationContext>{

	public void initialize(ConfigurableWebApplicationContext applicationContext) {
		
		
		// check for the SPARQLMAP_HOME
		String sparqlMapHome = System.getProperty("SPARQLMAP_HOME");
		if(sparqlMapHome!=null){
			Map<String,Properties> props = ContextSetup.readDirectory(new File(sparqlMapHome));
		
			for (String name : props.keySet()) {
				applicationContext.getEnvironment().getPropertySources().addFirst(new PropertiesPropertySource(name, props.get(name)));
			}
			
		}else{
			//check if the classpath contains a conf folder that has the properties and mappings
			throw new ImplementationException("Implement classpath lookup");
			
		}
		
	
		//read the files
	}
	
	
	

}
