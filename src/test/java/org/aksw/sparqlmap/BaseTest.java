package org.aksw.sparqlmap;

import java.io.File;

import org.aksw.sparqlmap.config.syntax.DBConnectionConfiguration;
import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.junit.Before;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;

public abstract class BaseTest {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BaseTest.class);
	
	R2RMLModel r2rmodel;

	private DBConnectionConfiguration dbConf;

	@Before
	public void setUp() throws Exception {
	String configLocation = "./src/main/conf/";
		File confFolder  = new File(configLocation);
		if(!confFolder.isDirectory()||!confFolder.exists()){
			log.error("no valid conf folder location given.");
			System.exit(0);
		}
		

		try {
			
			//First check the database connection
			
			File dbConfFile = new File(confFolder.getAbsolutePath() + "/db.properties");
			
			
			if(dbConfFile.isDirectory()||!dbConfFile.exists()){
				log.error("no file db.properties found in conf folder: " + confFolder.getAbsolutePath());
				System.exit(0);
			}
			
			this.dbConf = new  DBConnectionConfiguration(dbConfFile);
			
			
			//we now take the first ttl file in the folder as our mapping
			Model model = ModelFactory.createDefaultModel();
			for(File file: confFolder.listFiles()){
				if(file.getName().endsWith(".ttl")){
				//we now load all ttl files into a model. We assume, they are all mappings to be loaded
				log.info("Loading file: " + file.getAbsolutePath());
				FileManager.get().readModel(model, file.getAbsolutePath());
				}
			}
			
			//we now read the r2rml schema file
			
			Model schema = ModelFactory.createDefaultModel();
			FileManager.get().readModel(schema, confFolder.getAbsolutePath()+ "/r2rml.rdf");
			
			this.r2rmodel = new R2RMLModel(model, schema, dbConf);
			
			

		} catch (Exception e) {
			log.error("Error setting up the app", e);
			System.exit(0);
		}
	}
	
	
	
	
	abstract public String processQuery(String shortname, String query);

}
