package org.aksw.sparqlmap.bsbmtestcases;

import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMapFactory;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.FilterUtil;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import com.hp.hpl.jena.graph.NodeFactory;

public class BSBMPostgresTermMapTests {
	
	
	private ApplicationContext con;
	private SparqlMap r2r;
	private R2RMLModel r2rmlmodel;
	private FilterUtil filterUtil;
	private TermMapFactory tmf;

	@Before
	public void setupSparqlMap() {
		
		String pathToConf = "./src/main/resources/bsbm-test";
		
		con = ContextSetup.contextFromFolder(pathToConf);
		r2r = (SparqlMap) con.getBean("sparqlMap");
		r2rmlmodel =  (R2RMLModel)con.getBean(R2RMLModel.class);
		filterUtil = (FilterUtil) con.getBean(FilterUtil.class);
		tmf  = con.getBean(TermMapFactory.class);
		
	}
	
	
	@Test
	public void resourceTemplate_resourceTemplate(){
		// template is: http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer{producer}/Product{nr} 
		TermMap left = null;
		
		for(TripleMap trm : r2rmlmodel.getTripleMaps()){
			if(trm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/Product")){
				//get the subject
				left = trm.getSubject();
			}
		}
		
		
		TermMap right = tmf.createTermMap(NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1234/Product5678"));
		
		
		TermMap result = filterUtil.compareTermMaps(left, right, EqualsTo.class);
		
		Assert.assertTrue( DataTypeHelper.uncast(result.getLiteralValBool()) instanceof EqualsTo);
		
		
		
		
	}
	

}
