package org.aksw.sparqlmap.r2rmltestcases;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.aksw.sparqlmap.SparqlMap;
import org.aksw.sparqlmap.automapper.DB2R2RML;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;

import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

@RunWith(value = Parameterized.class)
public abstract class R2RMLTest extends R2RMLTestCaseAbstract{
	
	public static String baseUri = "http://example.com/base/";
	
	String testCaseName;
	String r2rmlLocation;
	String outputLocation;
	String referenceOutput;
	String dbFileLocation;
	boolean createDM;
	
	
	
	
	





	public R2RMLTest(String testCaseName, String r2rmlLocation,
			String outputLocation, String referenceOutput,
			String dbFileLocation, boolean createDM) {
		super();
		this.testCaseName = testCaseName;
		this.r2rmlLocation = r2rmlLocation;
		this.outputLocation = outputLocation;
		this.referenceOutput = referenceOutput;
		this.dbFileLocation = dbFileLocation;
		this.createDM = createDM;
	}


	@Test
	public void runTestcase() throws ClassNotFoundException, SQLException, IOException{
		flushDatabase();
		loadFileIntoDB(dbFileLocation);
		
		
		if(createDM){
			createDM(r2rmlLocation);
		}
		
		//let the mapper run.
		
		map();
	
		
		assertTrue(compare(outputLocation,referenceOutput));
		
	}


	private void map() throws SQLException, FileNotFoundException {
		AnnotationConfigApplicationContext ctxt = new AnnotationConfigApplicationContext();
		Properties sm = new Properties();
		sm.put("sm.baseuri", baseUri);
		//sm.put("sm.r2rmlvocablocation", new File("./src/main/resources/vocabularies/r2rml.ttl").getAbsolutePath());
		sm.put("sm.mappingfile", r2rmlLocation);
		
		ctxt.getEnvironment().getPropertySources().addFirst(new PropertiesPropertySource("sm", sm));
		ctxt.getEnvironment().getPropertySources().addFirst(new PropertiesPropertySource("db", getDBProperties()));
		
		ctxt.scan("org.aksw.sparqlmap");
		ctxt.refresh();
		
		SparqlMap r2r = ctxt.getBean(SparqlMap.class);
		r2r.dump(new FileOutputStream(new File(outputLocation)));
		ctxt.close();
	}
	
	
	public static Collection<Object[]> data(String tcFolder) {
		Collection<Object[]> testCases = new ArrayList<Object[]>();

		try {
			
			for(File folder: new File(tcFolder).listFiles()){
				if(folder.isDirectory()&&!folder.isHidden()){ 

				Model manifest = ModelFactory.createDefaultModel();
				manifest.read(new FileInputStream(new File(folder.getAbsolutePath()+"/manifest.ttl")), null,"TTL");
				
				
				
				
				// get the direct mapping test cases
				
				com.hp.hpl.jena.query.ResultSet dmRS = 
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
								"	rdb2rdftest:output ?outfname .\n" + 
								"   ?db rdb2rdftest:sqlScriptFile ?dbfile .\n" + 

								" } "),manifest).execSelect();
				
				while(dmRS.hasNext()){
					Binding bind = dmRS.nextBinding();
					String title = bind.get(Var.alloc("title")).getLiteral().toString();
					String identifier = bind.get(Var.alloc("identifier")).getLiteral().toString();
					String purpose = bind.get(Var.alloc("purpose")).getLiteral().toString();
					String reference = bind.get(Var.alloc("reference")).getLiteral().toString();
					String expectedOutput = bind.get(Var.alloc("expectedOutput")).getLiteral().toString();
					String outfname = bind.get(Var.alloc("outfname")).getLiteral().toString();
					String dbname = bind.get(Var.alloc("db")).getURI();
					String dbFileName = bind.get(Var.alloc("dbfile")).getLiteral().toString();
					
					testCases.add(new Object[]{identifier,makeAbsolute(folder, "dm_r2rml.ttl") ,getFileOutName(folder, outfname),makeAbsolute(folder, outfname),makeAbsolute(folder, dbFileName),true});
				}
				
				// get the regular test cases
				
				
				com.hp.hpl.jena.query.ResultSet r2rRs = 
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
								"	rdb2rdftest:mappingDocument ?mappingfname .\n" + 
								"   ?db rdb2rdftest:sqlScriptFile ?dbfile .\n" + 

								" } "),manifest).execSelect();
				
				while(r2rRs.hasNext()){				
					Binding bind = r2rRs.nextBinding();
					String title = bind.get(Var.alloc("title")).getLiteral().toString();
					String identifier = bind.get(Var.alloc("identifier")).getLiteral().toString();
					String purpose = bind.get(Var.alloc("purpose")).getLiteral().toString();
					String reference = bind.get(Var.alloc("reference")).getLiteral().toString();
					String expectedOutput = bind.get(Var.alloc("expectedOutput")).getLiteral().toString();
					String outfname = bind.get(Var.alloc("outfname")).getLiteral().toString();
					String mappingfname = bind.get(Var.alloc("mappingfname")).getLiteral().toString();
					String dbFileName = bind.get(Var.alloc("dbfile")).getLiteral().toString();

					testCases.add(new Object[]{identifier,makeAbsolute(folder, mappingfname) ,getFileOutName(folder, outfname),makeAbsolute(folder, outfname),makeAbsolute(folder, dbFileName),false});

					}
				}	
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return testCases;
		
	}
	
	
	
	
	public void createDM(String wheretowrite) throws ClassNotFoundException, SQLException, FileNotFoundException{
		Connection conn = getConnection();
		
		DB2R2RML db2r2rml = new DB2R2RML(conn, "http://example.com/base/mapping/", "http://example.com/base/data/", "http://example.com/base/vocab/",";");
		
		Model mapping = db2r2rml.getMydbData();
		conn.close();
		mapping.write(new FileOutputStream(new File(wheretowrite)), "TTL", null);
		
		
	}
	
	
	private static String makeAbsolute(File folder, String name){
		return folder.getAbsolutePath() + "/"  +name;
	}
	
	private static String getFileOutName(File folder, String outfname) {
		return folder.getAbsolutePath() + "/" + outfname.split("\\.")[0]
				+ "-sparqlmap." + outfname.split("\\.")[1];
	}
	
	

}
