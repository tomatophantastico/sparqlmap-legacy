//package org.aksw.sparqlmap;
//
//import java.io.File;
//
//import net.sf.jsqlparser.JSQLParserException;
//
//import org.aksw.sparqlmap.config.syntax.DBAccessConfigurator;
//import org.aksw.sparqlmap.config.syntax.IDBAccess;
//import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
//import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLValidationException;
//import org.aksw.sparqlmap.mapper.AlgebraBasedMapper;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
//import org.springframework.core.io.ClassPathResource;
//import org.springframework.core.io.Resource;
//
//import com.hp.hpl.jena.rdf.model.Model;
//import com.hp.hpl.jena.rdf.model.ModelFactory;
//import com.hp.hpl.jena.util.FileManager;
//
//
///**
// * This class ties together all components of SparqlMap and takes care of the configuration
// * @author joerg
// *
// */
//@Component
//public class SparqlMapConfiguration{
//	
//	
//	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SparqlMapConfiguration.class);
//	
//	public SparqlMapConfiguration(String configLocation ){
//	
//	
//	
//		
//		if(configLocation ==null){
//			configLocation = "./conf/";
//		}
//		File confFolder  = new File(configLocation);
//		if(!confFolder.isDirectory()||!confFolder.exists()){
//			log.error("no valid conf folder location given.");
//			System.exit(0);
//		}
//		
//
//		try {
//			
//			//First check the database connection
//			
//			File dbConfFile = new File(confFolder.getAbsolutePath() + "/db.properties");
//			
//			
//			if(dbConfFile.isDirectory()||!dbConfFile.exists()){
//				log.error("no file db.properties found in conf folder: " + confFolder.getAbsolutePath());
//				System.exit(0);
//			}
//			
//			DBAccessConfigurator dbaccConf = new DBAccessConfigurator(dbConfFile);
//			this.dbConf = dbaccConf.getDBAccess();
//			
//			
//			//we now take the first ttl file in the folder as our mapping
//			Model model = ModelFactory.createDefaultModel();
//			for(File file: confFolder.listFiles()){
//				if(file.getName().endsWith(".ttl")){
//				//we now load all ttl files into a model. We assume, they are all mappings to be loaded
//				log.info("Loading file: " + file.getAbsolutePath());
//				FileManager.get().readModel(model, file.getAbsolutePath());
//				}
//			}
//			
//			//we now read the r2rml schema file
//			
//			Model schema = ModelFactory.createDefaultModel();
//			FileManager.get().readModel(schema, confFolder.getAbsolutePath()+ "/r2rml.rdf");
//			
//			mapping = new R2RMLModel(model, schema, dbConf);
//			
//			mapper = new AlgebraBasedMapper(mapping,dbConf);
//
//		} catch (Exception e) {
//			log.error("Error setting up the app", e);
//			System.exit(0);
//		}
//
//	}
//	
//	
//	public SparqlMapConfiguration(IDBAccess dbconf, Model mapping, Model schema) throws R2RMLValidationException, JSQLParserException {
//		
//		this.dbConf = dbconf;
//		this.mapping = new R2RMLModel(mapping, schema, dbConf);
//		mapper = new AlgebraBasedMapper(this.mapping,dbConf);
//		
//		
//	}
//	
//	
//	
//
//}
