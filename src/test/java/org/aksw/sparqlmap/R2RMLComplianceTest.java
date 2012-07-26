package org.aksw.sparqlmap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.aksw.sparqlmap.config.syntax.DBConnectionConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.util.FileManager;

import db2r2ml.DB2R2RML;

public class R2RMLComplianceTest {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(R2RMLComplianceTest.class);
	DBConnectionConfiguration dbconn;
	
	Connection conn;
	
	@Before
	public void setUp() throws Exception {
		dbconn = new DBConnectionConfiguration(new File("./src/test/conf/db_r2rml.properties"));
		conn = dbconn.getConenction();
	}

	@Test
	public void test() throws Exception {
		
		for(File folder: new File("./src/test/rdb2rdf-tests/").listFiles()){
			if(folder.isDirectory()&&!folder.isHidden()){// &&folder.getName().contains("D019")){
			Model manifest = ModelFactory.createDefaultModel();
			
			manifest.read(new FileInputStream(new File(folder.getAbsolutePath()+"/manifest.ttl")), null,"TTL");
			
			//setup the db
			dbSetup(manifest, folder);
			
			executeR2RML(manifest,folder);
			
			executeDM(manifest,folder);
			
			
			}
			
			
			
		}
		
	}

	private void executeDM(Model manifest, File folder) {
		com.hp.hpl.jena.query.ResultSet spRs = 
				QueryExecutionFactory.create(QueryFactory.create("PREFIX test: <http://www.w3.org/2006/03/test-description#> \n" + 
						"PREFIX dcterms: <http://purl.org/dc/elements/1.1/> \n" + 
						"PREFIX  rdb2rdftest: <http://purl.org/NET/rdb2rdf-test#> " +
						"SELECT * WHERE {\n " +
						"   ?tc a rdb2rdftest:DirectMapping ;	\n" + 
						"	dcterms:title ?title ; \n" + 
						"	dcterms:identifier ?identifier ;\n" + 
						"	test:purpose ?purpose ;\n" + 
						"	test:specificationReference ?reference ;\n" + 
						"	test:reviewStatus ?reviewStatus ;\n" + 
						"	rdb2rdftest:hasExpectedOutput ?expectedOutput ;\n" + 
						"	rdb2rdftest:database ?db ;\n" + 
						"	rdb2rdftest:output ?outfname ;\n" + 
						". } "),manifest).execSelect();
		if(!spRs.hasNext()){
			log.info("No DM for this folder");
		}
			while(spRs.hasNext()){
						
						Binding bind = spRs.nextBinding();
						String title = bind.get(Var.alloc("title")).getLiteral().toString();
						String identifier = bind.get(Var.alloc("identifier")).getLiteral().toString();
						String purpose = bind.get(Var.alloc("purpose")).getLiteral().toString();
						String reference = bind.get(Var.alloc("reference")).getLiteral().toString();
						String expectedOutput = bind.get(Var.alloc("expectedOutput")).getLiteral().toString();
						String outfname = bind.get(Var.alloc("outfname")).getLiteral().toString();
						String dbname = bind.get(Var.alloc("db")).getURI();
						
						try {
							
							log.info("Executing direct mapping for: " + dbname.toString());

							Model schema = ModelFactory.createDefaultModel();
							FileManager.get().readModel(schema, "./src/main/conf/r2rml.rdf");
					
							
							DB2R2RML db2r2rml = new DB2R2RML();
							
							String dburl = "http:" + dbconn.getDbConnString().split(":")[2]+ "/";
							Connection conn = dbconn.getConenction();
							
							Model mapping = db2r2rml.getMydbData(conn, "http://example.com/base/", "http://example.com/base/", "http://example.com/base/");
							conn.close();
							
							mapping.write(new FileOutputStream(new File(folder + "/dm.r2rml")), "TTL", null);
							
							
							
							RDB2RDF r2r = new RDB2RDF(dbconn,mapping,schema);
							
							r2r.dump(new FileOutputStream(new File(getFileOutName(folder, outfname))));
						} catch (Exception e) {
							log.error("Error:",e);
							try {
								FileWriter errorwriter = new FileWriter(getFileOutName(folder, outfname)+".error");
								errorwriter.write(e.getLocalizedMessage());
							} catch (IOException e1) {
								// TODO Auto-generated catch block
								log.error("Error logging error:",e1);
							}
						}
			}
		
	}

	private void executeR2RML(Model manifest, File folder) {
		com.hp.hpl.jena.query.ResultSet spRs = 
				QueryExecutionFactory.create(QueryFactory.create("PREFIX test: <http://www.w3.org/2006/03/test-description#> \n" + 
						"PREFIX dcterms: <http://purl.org/dc/elements/1.1/> \n" + 
						"PREFIX  rdb2rdftest: <http://purl.org/NET/rdb2rdf-test#> " +
						"SELECT * WHERE {\n " +
						"   ?tc a rdb2rdftest:R2RML ;	\n" + 
						"	dcterms:title ?title ; \n" + 
						"	dcterms:identifier ?identifier ;\n" + 
						"	test:purpose ?purpose ;\n" + 
						"	test:specificationReference ?reference ;\n" + 
						"	test:reviewStatus ?reviewStatus ;\n" + 
						"	rdb2rdftest:hasExpectedOutput ?expectedOutput ;\n" + 
						"	rdb2rdftest:database ?db ;\n" + 
						"	rdb2rdftest:output ?outfname ;\n" + 
						"	rdb2rdftest:mappingDocument ?mappingfname ;\n" + 
						". } "),manifest).execSelect();
		
		while(spRs.hasNext()){
			
			Binding bind = spRs.nextBinding();
			String title = bind.get(Var.alloc("title")).getLiteral().toString();
			String identifier = bind.get(Var.alloc("identifier")).getLiteral().toString();
			String purpose = bind.get(Var.alloc("purpose")).getLiteral().toString();
			String reference = bind.get(Var.alloc("reference")).getLiteral().toString();
			String expectedOutput = bind.get(Var.alloc("expectedOutput")).getLiteral().toString();
			String outfname = bind.get(Var.alloc("outfname")).getLiteral().toString();
			String mappingfname = bind.get(Var.alloc("mappingfname")).getLiteral().toString();
			

				
			
			try {
				Model schema = ModelFactory.createDefaultModel();
				FileManager.get().readModel(schema, "./src/main/conf/r2rml.rdf");
				Model mapping = ModelFactory.createDefaultModel();
				log.info("Loading mapping " + folder.getAbsolutePath() + "/" + mappingfname);
				FileManager.get().readModel(mapping, folder.getAbsolutePath() + "/" + mappingfname);
				RDB2RDF r2r = new RDB2RDF(dbconn,mapping,schema);
				
				r2r.dump(new FileOutputStream(new File(getFileOutName(folder, outfname))));
			} catch (Exception e) {
				log.error("Error:",e);
				try {
					FileWriter errorwriter = new FileWriter(getFileOutName(folder, outfname) + ".error");
					errorwriter.write(e.getLocalizedMessage());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					log.error("Error logging error:",e1);
				}
			}
		}
	}
	
	private String getFileOutName(File folder,String outfname){
	
		return folder.getAbsolutePath() + "/"  +outfname.split("\\.")[0]+ "-sparqlmap." + outfname.split("\\.")[1];
		
		
	}

	private void dbSetup(Model manifest, File folder) throws SQLException, IOException {
		//remove all the old tables;
		java.sql.Statement stmt = conn.createStatement();
		ResultSet rs  = stmt.executeQuery("SELECT * FROM pg_tables WHERE tableowner ='" + dbconn.getUsername() + "'" );
		List<String> tablesToDelete = new ArrayList<String>(); 
		while(rs.next()){
			tablesToDelete.add(rs.getString("tablename"));
		}
		stmt.close();
		for (String tablename : tablesToDelete) {
			stmt = conn.createStatement();
			stmt.execute("DROP TABLE \"" + tablename +"\" CASCADE");
			stmt.close();
		}
		
		//now we execute all data from the create script;
		
		com.hp.hpl.jena.query.ResultSet spRs = QueryExecutionFactory.create(QueryFactory.create("SELECT ?title ?identifier ?file WHERE {\n " +
				"?db a <http://purl.org/NET/rdb2rdf-test#DataBase> . \n" +
				"?db <http://purl.org/NET/rdb2rdf-test#sqlScriptFile> ?file . \n" +
				"?db <http://purl.org/dc/elements/1.1/title> ?title .\n " + 
				"?db <http://purl.org/dc/elements/1.1/identifier> ?identifier \n } "),manifest).execSelect();
		
		
		QuerySolution sol = spRs.next();
		String title = sol.get("title").asLiteral().getString();
		String identifier =  sol.get("identifier").asLiteral().getString();
		String sqlFile = sol.get("file").asLiteral().getString();
		log.info("loading table for: " + identifier + " : " +title);
		
		String sql2Execute = FileUtils.readFileToString(new File(folder.getAbsoluteFile() + "/" + sqlFile));
		
		stmt = conn.createStatement();
		stmt.execute(sql2Execute);
		stmt.close();
				
	}
	
	
	@Test
	public void dump() throws SQLException{
		String dburl = "http:" + dbconn.getDbConnString().split(":")[2] + "/";
		DB2R2RML db2r2rml = new DB2R2RML();
		Model model=  db2r2rml.getMydbData(dbconn.getConenction(),dburl,dburl,dburl);
		model.write(System.out, "TTL", null);
		
	}

}
