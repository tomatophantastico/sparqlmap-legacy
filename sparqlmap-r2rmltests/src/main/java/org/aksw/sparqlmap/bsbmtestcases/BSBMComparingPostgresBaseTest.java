package org.aksw.sparqlmap.bsbmtestcases;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.WebContent;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.Multimap;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.resultset.ResultSetCompare;
import com.hp.hpl.jena.sparql.resultset.ResultSetMem;

public class BSBMComparingPostgresBaseTest {
	
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private SparqlMap sparqlMap;

	@Before
	public void setup(){
		String pathToConf = "./src/main/resources/bsbm-test";
		
		ApplicationContext con = ContextSetup.contextFromFolder(pathToConf);
		sparqlMap = (SparqlMap) con.getBean("sparqlMap");
	}
	
	public void executeAndCompare(String query) throws SQLException{
		
		
		Query queryObject = QueryFactory.create(query);

		QueryExecution qe = QueryExecutionFactory
				.sparqlService("http://localhost:8890/sparql", queryObject,
						"http://bsbm/100k");
			
		if (queryObject.isSelectType()) {
			
			ResultSetRewindable rsSparqlMap = new ResultSetMem(sparqlMap.executeSelect(query));

			ResultSetRewindable rsVirt = new ResultSetMem(qe.execSelect());

			if(ResultSetCompare.equalsByTerm(rsSparqlMap, rsVirt)){
				assertTrue(true);
			}else{
				rsSparqlMap.reset();
				rsVirt.reset();
				log.warn(" SparqlMap result: \n: "
						+ ResultSetFormatter.asText(rsSparqlMap));
				log.warn("Virtuoso result: \n "
						+ ResultSetFormatter.asText(rsVirt));
				assertTrue(false);
			}
	
		} else if (queryObject.isConstructType()) {
			
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			sparqlMap.executeSparql(query, WebContent.contentTypeTurtle,bos);
			Model smRes = ModelFactory.createDefaultModel();
			
			log.info("Content from SparqlMap construct: " + bos.toString() );
			
			RDFDataMgr.read(smRes, new  ByteArrayInputStream(bos.toByteArray()), Lang.TTL);
			Model virtRes = qe.execConstruct();
			
			
			assertTrue(virtRes.isIsomorphicWith(smRes));
		} else if(queryObject.isDescribeType()){
			
			
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			sparqlMap.executeSparql(query, WebContent.contentTypeTurtle,bos);
			Model smRes = ModelFactory.createDefaultModel();
			
			log.info("Content from SparqlMap construct: " + bos.toString() );
			
			RDFDataMgr.read(smRes, new  ByteArrayInputStream(bos.toByteArray()), Lang.TTL);
			Model virtRes = qe.execDescribe();
			
			
			assertTrue(virtRes.isIsomorphicWith(smRes));
			
		}
		
		

	}

	

}
