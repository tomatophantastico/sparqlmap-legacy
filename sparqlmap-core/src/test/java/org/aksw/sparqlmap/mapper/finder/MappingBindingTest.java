package org.aksw.sparqlmap.mapper.finder;

import static org.junit.Assert.assertTrue;

import org.aksw.sparqlmap.BSBMBaseTest;
import org.aksw.sparqlmap.core.beautifier.SparqlBeautifier;
import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.core.mapper.finder.Binder;
import org.aksw.sparqlmap.core.mapper.finder.FilterFinder;
import org.aksw.sparqlmap.core.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.core.mapper.finder.QueryInformation;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Op;

public class MappingBindingTest extends BSBMBaseTest {

	
	private SparqlBeautifier beautifier = new SparqlBeautifier();
	R2RMLModel model;
	
	Logger log = LoggerFactory.getLogger(MappingBindingTest.class);
	
	
	@Before
	public void setUp() throws Exception {
		super.setUp();
		
		
		model = (R2RMLModel) this.con.getBean(R2RMLModel.class);
		
	}


	public static String q1_reduced = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  " +
			"" +
			"SELECT DISTINCT ?product ?label WHERE {      ?product rdfs:label ?label .     " +
			"?product bsbm:productPropertyNumeric1 ?value1 .  	FILTER (?value1 > 136)  	} ORDER BY ?label LIMIT 10 ";


	
	

	@Test
	public void testq1() {
		
		Op op = this.beautifier.compileToBeauty(QueryFactory.create(q1_reduced)); // new  AlgebraGenerator().compile(beautified);
		log.info(op.toString());
		
		QueryInformation qi = FilterFinder.getQueryInformation(op);
		
				
		Binder binder = new Binder(model, qi);
		
		
		
		MappingBinding queryBinding = binder.bind(op);
		
		for(Triple triple : queryBinding.getBindingMap().keySet()){
			for(TripleMap tm : queryBinding.getBindingMap().get(triple)){
				for(PO po : tm.getPos()){
					if(triple.toString().contains("label")){
						
					} else if (triple.toString().contains("value1")){
						
					}else{
						//should not happen
						assertTrue(false);
					}
					
				}
				
				
			}
		}
		
		
		log.info(queryBinding.toString());
		
	}

	
	@Override
	public String processQuery(String shortname, String query) {
		// TODO Auto-generated method stub
		return null;
	}
}
