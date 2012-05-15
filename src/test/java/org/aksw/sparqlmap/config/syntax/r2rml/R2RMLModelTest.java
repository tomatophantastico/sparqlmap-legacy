package org.aksw.sparqlmap.config.syntax.r2rml;

import static org.junit.Assert.*;

import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;

import org.junit.Before;
import org.junit.Test;

public class R2RMLModelTest {
	
	R2RMLModel model;

	@Before
	public void setUp() throws Exception {
		model = new R2RMLModel("mapping/bsbmr2rml.ttl");
	}

	@Test
	public void test() throws R2RMLValidationException, JSQLParserException {
		
	Set<TripleMap> maps  = model.getTripleMaps();
		
		fail("Not yet implemented");
	}

}
