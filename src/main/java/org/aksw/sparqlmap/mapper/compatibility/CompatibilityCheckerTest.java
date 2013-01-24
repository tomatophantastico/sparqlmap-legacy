package org.aksw.sparqlmap.mapper.compatibility;

import static org.junit.Assert.*;

import org.aksw.sparqlmap.BSBMBaseTest;
import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap.PO;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.sparql.util.NodeFactory;

public class CompatibilityCheckerTest extends BSBMBaseTest {
	
	
	R2RMLModel model;

	@Before
	public void setUp() throws Exception {
		super.setUp();
		
		
		model = (R2RMLModel) this.con.getBean(R2RMLModel.class);
		
	}

	@Test
	public void testUriCompatibilityProductType() {
		
		Node toTest = Node_URI.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1446");

		for(TripleMap tm : model.getTripleMaps()){
			boolean sIsComp = tm.getSubject().getCompChecker().isCompatible(toTest);
			
			if(tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/ProductType")){
				assertTrue(sIsComp);
			}else{
				assertTrue(!sIsComp);
			}
			for(PO po: tm.getPos()){
				boolean pIsComp = po.getPredicate().getCompChecker().isCompatible(toTest);
				assertTrue(!pIsComp);
				boolean oIsComp = po.getObject().getCompChecker().isCompatible(toTest);
				if((tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/ProductTypeProduct")
						&& po.getPredicate().getResourceExpression().toString().contains("type"))
						||(tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/ProductType")&&po.getPredicate().getResourceExpression().toString().contains("subClassOf"))){
					assertTrue(oIsComp);
				}else{
					assertTrue(!oIsComp);
				}
			}
		}

	}
	
	@Test
	public void testUriCompatibilityProduct(){
		Node toTest = Node_URI.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer123/Product446");

		for(TripleMap tm : model.getTripleMaps()){
			boolean sIsComp = tm.getSubject().getCompChecker().isCompatible(toTest);
			
			if(tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/ProductTypeProduct")
					|| tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/Product")
					|| tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/ProductFeatureProduct")){
				assertTrue(sIsComp);
			}else{
				assertTrue(!sIsComp);
			}
			for(PO po: tm.getPos()){
				boolean pIsComp = po.getPredicate().getCompChecker().isCompatible(toTest);
				assertTrue(!pIsComp);
				boolean oIsComp = po.getObject().getCompChecker().isCompatible(toTest);
				if((tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/Review")
						&& po.getPredicate().getResourceExpression().toString().contains("product"))
						|| tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/Offer")
						&& po.getPredicate().getResourceExpression().toString().contains("product")){
					assertTrue(oIsComp);
				}else{
					assertTrue(!oIsComp);
				}
			}
		}
		
		
	}
	
	
	
	@Override
	public String processQuery(String shortname, String query) {
		// TODO Auto-generated method stub
		return null;
	}

}
