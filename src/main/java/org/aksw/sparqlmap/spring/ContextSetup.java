package org.aksw.sparqlmap.spring;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.io.ClassPathResource;

public abstract class ContextSetup {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ContextSetup.class);

	
	
	public static ApplicationContext contextFromProperties(Map<String,Properties> name2properiesMap){
		AnnotationConfigApplicationContext ctxt = new AnnotationConfigApplicationContext();
		
		
		for(String name : name2properiesMap.keySet()){
			ctxt.getEnvironment().getPropertySources().addFirst(new PropertiesPropertySource(name, name2properiesMap.get(name)));
		}
		
		
		
		ctxt.scan("org.aksw.sparqlmap");
		ctxt.refresh();
		return ctxt;
	}
	
	
	public static ApplicationContext contextFromFolder(String folder){
		
		
		
		

		return contextFromProperties(readDirectory(new File(folder)));
		
		
	}
	
	public static Map<String, Properties> readDirectory(File dir){
		try {
		Map<String, Properties> props = new HashMap<String, Properties>();
		if(!dir.exists()||!dir.isDirectory()){
			log.info("could not read from " + dir + ", as it is not existant or not a directory");
		}
		//read sparqlmap properties file

		Properties smprops = new Properties();
		
			smprops.load(new FileInputStream(dir.getAbsolutePath() + "/sparqlmap.properties"));
		
		
		//check for the mapping file;
		String filename = smprops.getProperty("sm.mappingfile");
		
		if(filename.startsWith("classpath:")){
			ClassPathResource mappingfileresource = new ClassPathResource(filename.substring(10));
			
			
				filename = mappingfileresource.getFile().getAbsolutePath();
			
		}
		
		if(!new File(filename).exists()){
			//file does not exist, so we make it relative to the folder we are in
			filename = dir.getAbsolutePath() +"/"+ filename;
		}
		
		
		//still not there? we try to find it in the classpath
		if(!new File(filename).exists()){
			String origfilname =  smprops.getProperty("sm.mappingfile");
			if(ContextSetup.class.getResource(origfilname)!=null){
				filename =  ContextSetup.class.getResource(origfilname).toString();
			}
		}

		if(!new File(filename).exists()){
			return null;
		}
		smprops.setProperty("sm.mappingfile", filename);
		if(!smprops.isEmpty()){
		props.put("sm",smprops);
		}
		
		Properties dbprops = new Properties();
		
			dbprops.load(new FileInputStream(dir.getAbsolutePath() + "/db.properties"));
		
		
		if(!dbprops.isEmpty()){
			props.put("db", dbprops);
		}
		
		
		if(!props.isEmpty()){
			return props;
		}else{
			return null;
		}
		
		
		} catch (FileNotFoundException e) {
			log.info("Error:",e);
			return null;
		} catch (IOException e) {
			log.info("Error:",e);
			return null;
		}
		
		
	}
	
}
