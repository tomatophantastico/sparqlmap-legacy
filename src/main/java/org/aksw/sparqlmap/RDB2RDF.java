package org.aksw.sparqlmap;

import java.io.File;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.DBConnectionConfiguration;
import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.db.SQLResultSetWrapper;
import org.aksw.sparqlmap.mapper.AlgebraBasedMapper;
import org.aksw.sparqlmap.mapper.Mapper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;
import org.openjena.riot.system.JenaWriterRdfJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.syntax.Template;
import com.hp.hpl.jena.sparql.util.ModelUtils;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.xmloutput.impl.Basic;

public class RDB2RDF {

	public Mapper mapper;
	
	private R2RMLModel mapping;
	
	private DBConnectionConfiguration dbConf;
	

	private Logger log = LoggerFactory.getLogger(RDB2RDF.class);
	
	
	public RDB2RDF(String configLocation ){
		
		if(configLocation ==null){
			configLocation = "./conf/";
		}
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
			
			mapping = new R2RMLModel(model, schema, dbConf);
			
			mapper = new AlgebraBasedMapper(mapping,dbConf);

		} catch (Exception e) {
			log.error("Error setting up the app", e);
			System.exit(0);
		}

		
		
	}

	
	public enum ReturnType {JSON,XML}
	
	/**
	 * Takes some of the functionality of QueryExecutionbase
	 * @param query this gets executed
	 * @return the result as a string
	 * @throws SQLException 
	 */
	public void  executeSparql(String qstring, ReturnType rt, OutputStream out) throws SQLException{
		
		
		
		
		Query query = QueryFactory.create(qstring);
		if(query.isAskType()){
			throw new ImplementationException("Dont Ask");
		}
		if(query.isConstructType()){
			Model model = executeConstruct(rt,  query, com.hp.hpl.jena.sparql.graph.GraphFactory.makeJenaDefaultModel());
			writeModel(rt, out, model);
			
			
		}
		if(query.isSelectType()){
			SQLResultSetWrapper rs = executeSparql(query);
			switch (rt) {
			case JSON:
				ResultSetFormatter.outputAsJSON(out,rs);
				break;

			default:
				ResultSetFormatter.outputAsXML(out,rs);
				break;
			}
			
		}
		if(query.isDescribeType()){
			Model model = 	com.hp.hpl.jena.sparql.graph.GraphFactory.makeJenaDefaultModel();
			List<Node> iris =  query.getResultURIs();
			if((iris == null || iris.isEmpty())){
				Var var = query.getProjectVars().get(0);
				SQLResultSetWrapper rs = executeSparql(query);
				while(rs.hasNext()){
					Binding b = rs.nextBinding();
					iris.add(b.get(var));
				}
				
			}
			
			for (Node node : iris) {
				String con1 = "CONSTRUCT {?s ?p <"+node.getURI()+">} WHERE {?s ?p <"+node.getURI()+">}";
				executeConstruct(rt, QueryFactory.create(con1), model);
				String con2 = "CONSTRUCT {<"+node.getURI()+"> ?p ?o} WHERE {<"+node.getURI()+"> ?p ?o}";
				executeConstruct(rt, QueryFactory.create(con2),model);
			}
			writeModel(rt, out, model);
			
		}

		

	}


	private void writeModel(ReturnType rt, OutputStream out, Model model) {
		switch (rt) {
		case JSON:
			JenaWriterRdfJson writer = new JenaWriterRdfJson();
			writer.write(model, out, null);
			break;

		default:
			Basic xmlwriter = new Basic();
			xmlwriter.write(model, out, null);
			break;
		}
	}


	private Model executeConstruct(ReturnType rt, Query query, Model model)
			throws SQLException {
		//take the graph pattern and convert it into a select query.
		Template template = query.getConstructTemplate();
		query.setQueryResultStar(true);
		
	
		
		//execute it 
		SQLResultSetWrapper rs = executeSparql(query);
		
		//bind it

		while (rs.hasNext()) {
			Set<Triple> set = new HashSet<Triple>();
			Map<Node, Node> bNodeMap = new HashMap<Node, Node>();
			Binding binding = rs.nextBinding();
			template.subst(set, bNodeMap, binding);
			for (Triple t : set) {
				Statement stmt = ModelUtils.tripleToStatement(model, t);
				if (stmt != null)
					model.add(stmt);
			}
		}
		
		return model;
		
		
	}
	
	
	public SQLResultSetWrapper executeSparql(Query query) throws SQLException{

		String sql = mapper.rewrite(query);
		

		
		return dbConf.executeSQL(sql);
		
	}

	public Mapper getMapper() {
		return mapper;
	}
}
