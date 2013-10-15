package org.aksw.sparqlmap.core.mapper.compatibility;

import static org.junit.Assert.assertTrue;

import net.sf.jsqlparser.expression.Expression;

import org.aksw.sparqlmap.BSBMBaseTest;
import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TermMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap.PO;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class CompatibilityCheckerTest extends BSBMBaseTest {
	
	
	R2RMLModel model;

	@Before
	public void setUp() throws Exception {
		super.setUp();
		
		
		model = (R2RMLModel) this.con.getBean(R2RMLModel.class);
		
	}

	@Test
	public void testUriCompatibilityProductType() {
		
		Node toTest = NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1446");

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
						&& contains(po.getPredicate(),"type"))
						||(tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/ProductType") && contains(po.getPredicate(),"subClassOf"))){
					assertTrue(oIsComp);
				}else{
					assertTrue(!oIsComp);
				}
			}
		}

	}
	
	@Test
	public void testUriCompatibilityProduct(){
		Node toTest = NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer123/Product446");

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
						&& contains( po.getPredicate(),"reviewFor"))
						|| (tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/Offer"))
						&& (contains(po.getPredicate(),"product") || contains(po.getPredicate(), "offerWebpage"))
						|| ((tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/Producer")||tm.getUri().equals("http://aksw.org/Projects/sparqlmap/mappings/bsbm/Vendor"))
						&& contains(po.getPredicate(),"homepage"))){
					assertTrue(oIsComp);
				}else{
					assertTrue(!oIsComp);
				}
			}
		}
		
		
	}
	
	private boolean contains(TermMap tm, String string){
		StringBuffer buff = new StringBuffer();
		for(Expression expr: tm.getExpressions()){
			buff.append(expr.toString());
		}
		
		return buff.toString().contains(string);
	}
	
	
	
	@Override
	public String processQuery(String shortname, String query) {
		// TODO Auto-generated method stub
		return null;
	}

}
