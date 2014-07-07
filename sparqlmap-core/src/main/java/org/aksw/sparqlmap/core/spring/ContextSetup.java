package org.aksw.sparqlmap.core.spring;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.aksw.sparqlmap.core.SystemInitializationError;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.io.ClassPathResource;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public abstract class ContextSetup {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ContextSetup.class);

	
	
	
	public static ApplicationContext contextFromProperties(Map<String,Properties> name2properiesMap){
		AnnotationConfigApplicationContext ctxt = new AnnotationConfigApplicationContext();
		
		
		for(String name : name2properiesMap.keySet()){
			ctxt.getEnvironment().getPropertySources().addFirst(new PropertiesPropertySource(name, name2properiesMap.get(name)));
		}
		
		
		
		ctxt.scan("org.aksw.sparqlmap.core");
		ctxt.refresh();
		return ctxt;
	}
	
	
	public static ApplicationContext contextFromFolder(String folder){
		
		return contextFromFolder(folder, "sparqlmap.properties");
	}
	public static ApplicationContext contextFromFolder(String folder, String mainConfFile){
		
		return contextFromProperties(readDirectory(new File(folder), mainConfFile));
	}
	
	public static Map<String, Properties> readDirectory(File dir,
			String mainConfFile) {
		log.info("Using setup dir : " + dir.getAbsolutePath());

		try {
			Map<String, Properties> props = new HashMap<String, Properties>();
			if (!dir.exists() || !dir.isDirectory()) {
				log.error("could not read from " + dir
						+ ", as it is not existant or not a directory");
			}
			// read sparqlmap properties file
			
			File mainFile = new File(dir.getAbsolutePath() + "/"+ mainConfFile);
			if(!mainFile.exists()){
				log.error("Main config file " + mainFile.getAbsolutePath() + " does not exist." );
			}
			log.info("Using " + mainFile.getAbsolutePath()  + " as main config.");	
			Properties smprops = new Properties();
			Reader reader = Files.newReader(mainFile, Charsets.UTF_8);
			smprops.load(reader);
			
			log.info("Read "+  smprops.size() +  " properties from " + mainFile.getAbsolutePath());
			

			// check for the mapping file;
			String filename = smprops.getProperty("sm.mappingfile");
			if (filename.startsWith("classpath:")) {
				ClassPathResource mappingfileresource = new ClassPathResource(
						filename.substring(10));
				filename = mappingfileresource.getFile().getAbsolutePath();

			}

			if (!new File(filename).exists()) {
				// file does not exist, so we make it relative to the folder we
				// are in
				filename = dir.getAbsolutePath() + "/" + filename;
			}

			// still not there? we try to find it in the classpath
			if (!new File(filename).exists()) {
				String origfilname = smprops.getProperty("sm.mappingfile");
				if (ContextSetup.class.getResource(origfilname) != null) {
					filename = ContextSetup.class.getResource(origfilname)
							.toString();
				}
			}

			if (!new File(filename).exists()) {
				log.error("Unable to locate mapping file: " + smprops.getProperty("sm.mappingfile") );
				return null;
			}
			smprops.setProperty("sm.mappingfile", filename);
			if (!smprops.isEmpty()) {
				props.put("sm", smprops);
			}

			Properties dbprops = new Properties();
			Reader dbreader = Files.newReader(new File(dir.getAbsolutePath()
					+ "/db.properties"), Charsets.UTF_8);
			dbprops.load(dbreader);

			if (!dbprops.isEmpty()) {
				props.put("db", dbprops);
			}

			if (!props.isEmpty()) {
				return props;
			} else {
				return null;
			}

		} catch (IOException e) {
			log.error("Unable to find file: " + e.getLocalizedMessage(), e);
			throw new SystemInitializationError("Unable to find file: "
					+ e.getLocalizedMessage());
		}

	}
	
}
