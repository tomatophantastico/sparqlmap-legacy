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
import java.util.Properties;

import org.aksw.sparqlmap.automapper.DB2R2RML;
import org.aksw.sparqlmap.db.DBAccess;
import org.aksw.sparqlmap.db.DBAccessConfigurator;
import org.apache.commons.io.FileUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;

import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.util.FileManager;


public class R2RMLComplianceTest {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(R2RMLComplianceTest.class);
	DBAccess dbSetupConn;
	
	Properties dbprops;
	Properties smprops;
	String baseUri = "http://example.com/test/";
	
	
	
	public static void main(String[] args) throws Exception {
		
		if(args.length!=4){
			System.out.println("requires parameters: <dburl> <dbusername> <dbpassword> <testSuiteFolder>");
			System.exit(0);
		}
		
		
		R2RMLComplianceTest test =  new R2RMLComplianceTest();
		
		Properties dbprops = new Properties();
		
		dbprops.setProperty("jdbc.url",args[0]);
		 
		dbprops.setProperty("jdbc.username",args[1]);
		dbprops.setProperty("jdbc.password",args[2]);
		dbprops.setProperty("jdbc.poolminconnections","5");
		dbprops.setProperty("jdbc.poolmaxconnections","10");
		
		test.dbprops = dbprops;
		
		
		Properties setupProps = new Properties();
		setupProps.putAll(dbprops);
		if(setupProps.getProperty("jdbc.url").startsWith("jdbc:mysql")){
			String jdbcurl = setupProps.getProperty("jdbc.url") +"?sessionVariables=sql_Mode=ANSI&allowMultiQueries=true";
			setupProps.setProperty("jdbc.url",jdbcurl);
		}
		
		
		DBAccessConfigurator dbaconf = new  DBAccessConfigurator(setupProps);
		test.dbSetupConn = dbaconf.getDBAccess();
		
		test.test(args[3]);
		
	}
	



	public void test(String testSuiteFolder) throws Exception {
		
		for(File folder: new File(testSuiteFolder).listFiles()){
			if(folder.isDirectory()&&!folder.isHidden() &&folder.getName().startsWith("D017")){ //25, , done: 17, 16
	
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
							FileManager.get().readModel(schema, "./src/main/resources/vocabularies/r2rml.ttl");
					
							Connection conn = dbSetupConn.getConnection();

							DB2R2RML db2r2rml = new DB2R2RML(conn, "http://example.com/base/mapping/", "http://example.com/base/data/", "http://example.com/base/vocab/",";");
							
							
							//String dburl = "http:" + dbconn.getDbConnString().split(":")[2]+ "/";
							
							Model mapping = db2r2rml.getMydbData();
							conn.close();
							
							mapping.write(new FileOutputStream(new File(folder + "/dm_r2rml.ttl")), "TTL", null);
							
							// gather the properties data
							Properties sm = new Properties();
							sm.put("sm.baseuri", this.baseUri);
							sm.put("sm.r2rmlvocablocation", new File("./src/main/resources/vocabularies/r2rml.ttl").getAbsolutePath());
							sm.put("sm.mappingfile", new File(folder + "/dm_r2rml.ttl").getAbsolutePath());
							log.info("Loading mapping " +  new File(folder+ "/dm_r2rml.ttl").getAbsolutePath());
							
							AnnotationConfigApplicationContext ctxt = setupSparqlMap(this.dbprops, sm);
							
							SparqlMap r2r = ctxt.getBean(SparqlMap.class);
							
							r2r.dump(new FileOutputStream(new File(getFileOutName(folder, outfname))));

							
							ctxt.close();
							
						} catch (Exception e) {
							log.error("Error:",e);
							try {
								FileWriter errorwriter = new FileWriter(getFileOutName(folder, outfname)+".error");
								errorwriter.write(e.getLocalizedMessage()+"");
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
				// gather the properties data
				Properties sm = new Properties();
				sm.put("sm.baseuri", this.baseUri);
				sm.put("sm.r2rmlvocablocation", new File("./src/main/resources/vocabularies/r2rml.ttl").getAbsolutePath());
				sm.put("sm.mappingfile", folder.getAbsolutePath() + "/" + mappingfname);
				log.info("Loading mapping " +  folder.getAbsolutePath() + "/" + mappingfname);
				
							
				AnnotationConfigApplicationContext ctxt = setupSparqlMap(this.dbprops, sm);
				
				SparqlMap r2r = ctxt.getBean(SparqlMap.class);
				
				r2r.dump(new FileOutputStream(new File(getFileOutName(folder, outfname))));
				
				ctxt.close();
				
				
				
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

	public void dbSetup(Model manifest, File folder) throws SQLException, IOException {
		Connection conn = this.dbSetupConn.getConnection();
		
		
		ResultSet res =  conn.getMetaData().getTables(null, null, null, new String[] {"TABLE"});
		List<String> tablesToDelete = new ArrayList<String>(); 
		while(res.next()){
			String tcat  = res.getString("TABLE_CAT"); 
	          String tschem =res.getString("TABLE_SCHEM");
	           String tname = res.getString("TABLE_NAME");
	           String ttype = res.getString("TABLE_TYPE");
	           String tremsarks = res.getString("REMARKS");
	           tablesToDelete.add(tname);
		}
		
		
		for (String tablename : tablesToDelete) {
			java.sql.Statement stmt = conn.createStatement();
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
		
		java.sql.Statement stmt = conn.createStatement();
		try {
			stmt.execute(sql2Execute);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("Error executing query: " +sql2Execute, e);
			throw new RuntimeException(e);
		}
		stmt.close();
		conn.close();
				
	}

	
	private AnnotationConfigApplicationContext setupSparqlMap(Properties db, Properties sm){
		
		
		
		AnnotationConfigApplicationContext ctxt = new AnnotationConfigApplicationContext();
		
		
		ctxt.getEnvironment().getPropertySources().addFirst(new PropertiesPropertySource("sm", sm));
		ctxt.getEnvironment().getPropertySources().addFirst(new PropertiesPropertySource("db", db));
		
		ctxt.scan("org.aksw.sparqlmap");
		ctxt.refresh();
	
		
		
		
		
		
		return ctxt;
	}

}
