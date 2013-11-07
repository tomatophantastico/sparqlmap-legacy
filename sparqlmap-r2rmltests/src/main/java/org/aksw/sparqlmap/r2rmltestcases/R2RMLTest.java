package org.aksw.sparqlmap.r2rmltestcases;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.automapper.Automapper;
import org.aksw.sparqlmap.core.automapper.AutomapperWrapper;
import org.aksw.sparqlmap.core.db.Connector;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.WebContent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

@RunWith(value = Parameterized.class)
public abstract class R2RMLTest {
	
	public static String baseUri = "http://example.com/base/";
	
	String testCaseName;
	String r2rmlLocation;
	String outputLocation;
	String referenceOutput;
	String dbFileLocation;
	boolean createDM;
	
	
	
	
	private static Logger log = LoggerFactory.getLogger(R2RMLTest.class);





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
		r2r.dump(new FileOutputStream(new File(outputLocation)),WebContent.contentTypeNTriples);
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
		Connection conn = getConnector().getConnection();
		
		
		
		Automapper db2r2rml = new Automapper(conn, "http://example.com/base/", "http://example.com/base/", "http://example.com/base/",";");
		
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
	
	

	/**
	 * returns a brand new Database connection which must be closed afterwards
	 * @return
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public abstract Connector getConnector();
	
	
	/**
	 * creates the properties to put into the spring container.
	 * @return
	 */
	public abstract Properties getDBProperties();
	
	/**
	 * closes the connection
	 * @param conn
	 */
	public void closeConnection(Connection conn){
		//crappy connection handling is ok here.
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * load the file into the database
	 * @param file
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public void loadFileIntoDB(String file) throws ClassNotFoundException, SQLException, IOException{
		ResourceDatabasePopulator rdp = new ResourceDatabasePopulator();
		
		rdp.addScript(new FileSystemResource(file));
		Connection conn = getConnector().getConnection();
		conn.setAutoCommit(true);
		rdp.populate(conn);
		conn.close();

//		String sql2Execute = FileUtils.readFileToString(new File(file));
//		loadStringIntoDb(sql2Execute);

		
	}


	public void loadStringIntoDb(String sql2Execute)
			throws ClassNotFoundException, SQLException {
		Connection conn = getConnector().getConnection();
		conn.setAutoCommit(true);
	
		
		java.sql.Statement stmt = conn.createStatement();
		stmt.execute(sql2Execute);
		
		stmt.close();
		conn.close();
	}
	
	
	/**
	 * deletes all tables of the database
	 * @return true if delete was successfull
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public void flushDatabase() throws ClassNotFoundException, SQLException{
		List<String> tablesToDelete = getTablesInDb();
		Connection conn = getConnector().getConnection();

		// brute force delete of the tables int there
		for (String table : tablesToDelete) {

			try {

				java.sql.Statement stmt = conn.createStatement();
				stmt.execute("DROP TABLE \"" + table + "\" CASCADE");
				stmt.close();

			} catch (SQLException e) {
				log.info("brute force delete threw error, nothing unusual");
			}
		}

		conn.close();
		
	}
	
	public List<String> getTablesInDb() throws SQLException {
		List<String> tables = new ArrayList<String>(); 
		Connection conn = getConnector().getConnection();
		ResultSet res =  conn.getMetaData().getTables(null, null, null, new String[] {"TABLE"});
		while(res.next()){
			String tcat  = res.getString("TABLE_CAT"); 
	          String tschem =res.getString("TABLE_SCHEM");
	           String tname = res.getString("TABLE_NAME");
	           String ttype = res.getString("TABLE_TYPE");
	           String tremsarks = res.getString("REMARKS");
	           tables.add(tname);
		}
		conn.close();
		return tables;
	}


	/**
	 * compares the two files for equality
	 * @param outputLocation2
	 * @param referenceOutput2
	 * @return true if they are equal
	 * @throws FileNotFoundException 
	 */
	
	public boolean compare(String outputLocation, String referenceOutput) throws FileNotFoundException {
		
		Model m1 = ModelFactory.createDefaultModel();
		String fileSuffixout = outputLocation.substring(outputLocation.lastIndexOf(".")+1).toUpperCase();
		
		if(fileSuffixout.equals("NQ")){
			DatasetGraph dsgout = RDFDataMgr.loadDatasetGraph(outputLocation);
			DatasetGraph dsdref = RDFDataMgr.loadDatasetGraph(referenceOutput);
			
			if (dsgout.isEmpty() != dsdref .isEmpty()){
				  return false;
			}
			
			Iterator<Node> iout = dsgout.listGraphNodes();
			Iterator<Node> iref = dsdref.listGraphNodes();
		
			    while (iout.hasNext())
			    {
			      Node outNode = (Node)iout.next();
			      Graph outgraph =  dsgout.getGraph(outNode);
			      Graph refGRaf = dsdref.getGraph(outNode);
			      if (!outgraph.isIsomorphicWith(refGRaf))
			        return false;
			    }
			    return true;
			    
		}else {
		//if(fileSuffixout.equals("TTL")){
			m1.read(new FileInputStream(outputLocation),null,"TTL");
			Model m2 = ModelFactory.createDefaultModel();
			m2.read(new FileInputStream(referenceOutput),null,"TTL");
			
			if(m1.isIsomorphicWith(m2)){
				return true;
			}else{
				return false;
			}
		}
		
	
	}
	

}
