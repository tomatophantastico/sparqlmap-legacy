package org.aksw.sparqlmap;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;

import org.aksw.sparqlmap.RDB2RDF.ReturnType;
import org.aksw.sparqlmap.mapper.AlgebraBasedMapper;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QueryFactory;

public class BSBMAlgebraRewriter {
	
	
		
	RDB2RDF r2r = new RDB2RDF("./src/main/conf");
	
	@Test
	public void query1Test() {
		
		
		String result = processQuery("q1", BSBMTest.q1);
		
		assertTrue(result.contains("dataFromProducer1092/Product54002"));
		assertTrue(result.contains("gobies tweet counteropening"));
		assertTrue(result.contains("dataFromProducer1905/Product95736"));
		assertTrue(result.contains("despatch"));
		assertTrue(result.contains("dataFromProducer1420/Product70737"));
		assertTrue(result.contains("insects"));
		assertTrue(result.contains("dataFromProducer1304/Product64824"));
		assertTrue(result.contains("torchbearers reenlightens"));
		assertTrue(StringUtils.countMatches(result, "<result>")==4);
		
		
	}
	
	
	@Test
	public void query2Test() {
		String result = processQuery("q2", BSBMTest.q2);
		assertTrue(result.contains("buzzards directories chinking traitress melodies apologetically gendering rethreaded rosins"));
		assertTrue(result.contains("undramatic"));
		assertTrue(StringUtils.countMatches(result, "<result>")==23);
	}
	
	@Test
	public void query3Test() {
		String result = processQuery("q3", BSBMTest.q3);
		assertTrue(result.contains("dataFromProducer1612/Product80526"));
		assertTrue(result.contains("dataFromProducer438/Product21639"));
		assertTrue(result.contains("bemata"));
		assertTrue(result.contains("espouses chips"));
		assertTrue(StringUtils.countMatches(result, "<result>")==2);
	}
	
	@Test
	public void query4Test() {
		
		String result = processQuery("q4", BSBMTest.q4);
		log.info(r2r.mapper.rewrite(QueryFactory.create(BSBMTest.q4)));
		assertTrue(result.contains("unfreeze"));
		assertTrue(result.contains("dataFromProducer39/Product1761"));
		assertTrue(StringUtils.countMatches(result, "<result>")==1);
	}
	
	
	
	@Test
	public void query5Test(){
		
		log.info(BSBMTest.q5);
		log.info(((AlgebraBasedMapper)r2r.mapper).getBeautifier().compileToBeauty(QueryFactory.create(BSBMTest.q5)).toString());
		log.info(r2r.mapper.rewrite(QueryFactory.create(BSBMTest.q5)));
		String result = processQuery("", BSBMTest.q5);
		
		assertTrue(StringUtils.countMatches(result, "<result>")==2);
		assertTrue(result.contains("democratizes micronesian"));
		log.info(result);
	}
	
	@Test
	public void query7Test() {
		String result = processQuery("q7", BSBMTest.q7);
		assertTrue(result.contains("dataFromRatingSite3/Review24503"));
		assertTrue(result.contains("dataFromRatingSite1/Review4242"));
		assertTrue(result.contains("farrier canonizations abuses"));
		assertTrue(result.contains("Luitgarda-Zaaid"));
		assertTrue(StringUtils.countMatches(result, "<result>")==16);
	}
	
	
	@Test
	public void query8Test() {
		String result = processQuery("q8", BSBMTest.q8);
		log.info(r2r.mapper.rewrite(QueryFactory.create(BSBMTest.q8)));
		assertTrue(result.contains("nonchalantly docility torques bloodies commensurations sandlots arsenical hags"));
		assertTrue(result.contains("Deejay"));
		assertTrue(result.contains("dataFromRatingSite1/Reviewer455"));
		assertTrue(result.contains("dataFromRatingSite1/Reviewer85"));
		assertTrue(result.contains("dataFromRatingSite2/Reviewer730"));
		assertTrue(StringUtils.countMatches(result, "<result>")==3);
	}
	
	
	
	
	@Test
	public void query9Test() {
		
		
		String result = processQuery("q9", BSBMTest.q9);
		assertTrue(result.contains("dataFromProducer1230/Product61268"));
		assertTrue(result.contains("dataFromProducer1593/Product79515"));
		assertTrue(result.contains("tusches misquotation"));
		assertTrue(result.contains("lassoed phenological feist"));
		assertTrue(StringUtils.countMatches(result, "<result>")==10);

	}
	
	@Test
	public void query10Test() {
		
		
		String result = processQuery("q10", BSBMTest.q10);
		log.info(r2r.mapper.rewrite(QueryFactory.create(BSBMTest.q10)));

		assertTrue(result.contains("6546.06"));
		assertTrue(result.contains("7263.04"));
		assertTrue(result.contains("dataFromVendor28/Offer52408"));
		assertTrue(result.contains("USD"));		
		assertTrue(StringUtils.countMatches(result, "<result>")==2);
		

	}
	
	@Test
	public void query11Test() {
		String result = processQuery("q11", BSBMTest.q11);
		assertTrue(result.contains("dataFromProducer1230/Product61268"));
		assertTrue(result.contains("dataFromProducer1593/Product79515"));
		assertTrue(result.contains("tusches misquotation"));
		assertTrue(result.contains("lassoed phenological feist"));
		assertTrue(StringUtils.countMatches(result, "<result>")==10);
		

	}
	
	
	@Test
	public void query12Test() {
		
		
		String result = processQuery("q12", BSBMTest.q12);
		assertTrue(result.contains("http://www.vendor679.com/"));
		assertTrue(result.contains("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557/"));
		assertTrue(result.contains("2008-03-27T00:00:00"));
		assertTrue(result.contains("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1399/Product69682"));


	}
	
	
	
	
	private Logger log = LoggerFactory.getLogger(BSBMAlgebraRewriter.class);
	
	
	public String processQuery(String queryShortname, String queryString) {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			r2r.executeSparql(queryString,ReturnType.XML,result);
		} catch (SQLException e) {
			
			log.error("Error:",e);
		}
		
		String resString =result.toString(); 

		log.info(resString);
		
		return resString;
		
	}
	
	public void processQuery(String queryShortname, String query,
			String sqlQuery, ArrayList checkKeys) {
		processQuery(queryShortname, query);
	}

}
