package org.aksw.sparqlmap;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.aksw.sparqlmap.core.SparqlMap;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.WebContent;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.server.Server;
import org.junit.After;
import org.junit.Before;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.io.ClassPathResource;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.resultset.ResultSetCompare;
import com.hp.hpl.jena.sparql.resultset.ResultSetMem;
import com.hp.hpl.jena.sparql.resultset.ResultsFormat;
import com.hp.hpl.jena.sparql.util.ModelUtils;
import com.hp.hpl.jena.sparql.util.ResultSetUtils;
import com.sun.org.apache.bcel.internal.generic.CPInstruction;


/**
 * Base test for integration testing using HSQL.
 * 
 * @author joerg
 *
 */
public abstract class HSQLBaseTest {
	
	
	public AnnotationConfigApplicationContext context;
	Server server;
	public SparqlMap sparqlmap;
	
	
	
	@Before
	public void setup() throws Exception{
		
		String dbLocation =  "./target/" + getSQLFile() + "/hsqldb/db";
		File dbFile = new File(dbLocation);
		File dbTmpFile = new File(dbLocation + ".tmp");
		
		
		
		String jdbcurl =  "jdbc:hsqldb:file:" + dbLocation;
		String jdbcuser = "SA";
		String jdbcpass = "";
		
		if(!dbTmpFile.exists()){
			Connection conn =  DriverManager.getConnection(jdbcurl, jdbcuser, jdbcpass);
			SqlFile sqlFile = new SqlFile(new File(getSQLFile()));
			sqlFile.setConnection(conn);
			sqlFile.execute();
			conn.close();
		}
		
		// settin up SparqlMap
		
		Properties props = new Properties();
		props.load(new ClassPathResource("sparqlmap.properties").getInputStream());
		props.setProperty("jdbc.url",jdbcurl);
		props.setProperty("jdbc.username", jdbcuser);
		props.setProperty("jdbc.password",jdbcpass);
		props.setProperty("sm.mappingfile",getMappingFile());
		


		context = new AnnotationConfigApplicationContext();		
		context.getEnvironment().getPropertySources()
					.addFirst(new PropertiesPropertySource("testprops",props));
		context.scan("org.aksw.sparqlmap");
		context.refresh();
		sparqlmap =  context.getBean(SparqlMap.class);
		

	}
	
	@After
	public void  closeDatabase() throws SQLException{
		 Connection c = DriverManager.getConnection(
		         "jdbc:hsqldb:mem:testing;shutdown=true", "SA", "");
		
	}
	
	
	/**
	 * also checks for ordering
	 * @param sparqlSelect
	 * @param xmlResultSet
	 * @throws SQLException 
	 */
	public void executeAndCompareSelect(String sparqlSelect, File xmlResultSet){
		
		if(!xmlResultSet.exists()){
			createResultSet(sparqlSelect, xmlResultSet);
		}
		
		
		
		
		SparqlMap sm  = context.getBean(SparqlMap.class);
		
		try {
			
			boolean matches = ResultSetCompare.equalsByTermAndOrder(
					sm.executeSelect(sparqlSelect), 
					ResultSetFactory.load(
							xmlResultSet.getAbsolutePath(), 
							ResultsFormat.FMT_RS_JSON));
			
			StringBuffer comparison = new StringBuffer();
			if (!matches) {
				ResultSet result = sm.executeSelect(sparqlSelect);
				ResultSet expectedRS = ResultSetFactory.load(
						xmlResultSet.getAbsolutePath(),
						ResultsFormat.FMT_RS_JSON);

				comparison
						.append("Resultsets do no match, compare: \n Reference Set:\n");
				comparison.append(ResultSetFormatter.asText(expectedRS));
				comparison.append("\n===================== Executed Set:\n");
				comparison.append(ResultSetFormatter.asText(result));
			}
			assertTrue(comparison.toString(),matches);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}	
		
		
	}
	
	
	private void createResultSet(String sparqlSelect, File xmlResultSetFile){

		try {
			
			Dataset ds = DatasetFactory.create(sparqlmap.dump());
			ResultSet rs = QueryExecutionFactory.create(sparqlSelect, ds).execSelect();
			
			ResultSetFormatter.output(new FileOutputStream(xmlResultSetFile), rs, ResultsFormat.FMT_RS_JSON);
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	
	public void executeAndCompareConstruct(String sparqlConstruct, String resultmodelLocation) throws SQLException{
		Model expectedResult = ModelFactory.createDefaultModel();
		expectedResult.read(resultmodelLocation);
		executeAndCompareConstruct(sparqlConstruct, expectedResult);
	}
	
	public void executeAndCompareConstruct(String sparqlConstruct, Model expectedresult) throws SQLException{
		
		SparqlMap sm  = context.getBean(SparqlMap.class);
		
		Model result = sm.executeConstruct(sparqlConstruct);
		
		StringBuffer models =new StringBuffer();
		
		models.append("Actual result is :\n");
		models.append("=============================");
		ByteArrayOutputStream actualResBos  = new ByteArrayOutputStream();
		RDFDataMgr.write(actualResBos, result,Lang.TURTLE);
		models.append(actualResBos);
		
		models.append("=======================\nExpected was: ");
		ByteArrayOutputStream expectedResBos  =new ByteArrayOutputStream();
		RDFDataMgr.write(expectedResBos, expectedresult,Lang.TURTLE);
		models.append(expectedResBos);
		models.append("=============================");
		
		
		
		
		assertTrue(models.toString(), result.isIsomorphicWith(expectedresult));
	
		
		
		
		
	}
	
	public abstract String getMappingFile();
	public abstract String getSQLFile();
	public abstract String getTestName();

}
