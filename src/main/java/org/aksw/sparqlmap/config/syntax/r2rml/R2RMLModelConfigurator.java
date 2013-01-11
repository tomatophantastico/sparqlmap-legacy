package org.aksw.sparqlmap.config.syntax.r2rml;

import java.io.IOException;
import java.sql.SQLException;

import javax.annotation.PostConstruct;

import net.sf.jsqlparser.JSQLParserException;

import org.aksw.sparqlmap.automapper.AutomapperWrapper;
import org.aksw.sparqlmap.db.IDBAccess;
import org.aksw.sparqlmap.mapper.translate.DataTypeHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;

@Component
public class R2RMLModelConfigurator {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(R2RMLModelConfigurator.class);
	
	String mappingfile;
	
	String r2rmlvocabmodel;
	
	
	@Autowired
	AutomapperWrapper automapper; 
	
	@Autowired
	IDBAccess dbaccess;
	@Autowired
	private ColumnHelper columnhelper;
	@Autowired
	private DataTypeHelper dth;
	
	@Autowired
	Environment env;
	
	
	@PostConstruct
	public void setValues(){
		mappingfile = env.getProperty("sm.mappingfile");
		r2rmlvocabmodel = env.getProperty("sm.r2rmlvocablocation");
		
	}
	
	@Bean
	public R2RMLModel createModel() throws R2RMLValidationException, JSQLParserException, SQLException, IOException{
		
		Model schema = ModelFactory.createDefaultModel();
		if(r2rmlvocabmodel!=null){
			
			if(r2rmlvocabmodel.startsWith("classpath:")){
				ClassPathResource vocab = new ClassPathResource(r2rmlvocabmodel.substring(10));
				schema.read(vocab.getInputStream(),null, "TTL");
				
			}else{
				FileManager.get().readModel(schema,r2rmlvocabmodel);
			}
			
		
		}else{
			FileManager.get().readModel(schema,"vocabularies/r2rml.ttl");
		}
		Model mapping = ModelFactory.createDefaultModel();

		if(mappingfile!=null){
			log.info("Loading mapping " + mappingfile);
			mapping = FileManager.get().readModel(mapping, mappingfile);
		}else{
			log.info("Using direct mapping");
			mapping = automapper.automap();
		}
		
		
		
		
		R2RMLModel model = new R2RMLModel(columnhelper,dbaccess,dth,mapping,schema);
		
		return model;
	}

}
