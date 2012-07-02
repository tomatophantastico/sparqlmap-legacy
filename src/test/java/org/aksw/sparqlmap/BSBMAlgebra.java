package org.aksw.sparqlmap;

import java.io.InputStreamReader;
import java.util.ArrayList;

import org.aksw.sparqlmap.config.syntax.SparqlMapConfiguration;
import org.aksw.sparqlmap.config.syntax.SimpleConfigParser;
import org.aksw.sparqlmap.db.SQLAccessFacade;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.AlgebraBasedMapper;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QueryFactory;

public class BSBMAlgebra extends BSBMTest {
	
	
	
	
	
	
	
	private AlgebraBasedMapper mapper;

	@Before
	public void initMapper() throws Throwable {

		SimpleConfigParser parser = new SimpleConfigParser();

		SparqlMapConfiguration config = parser.parse(new InputStreamReader(
				ClassLoader.getSystemResourceAsStream("bsbm.r2rml")));

		mapper = new AlgebraBasedMapper(config);

		db = new SQLAccessFacade(config.getDbConn());

		mapper.setConn(db);

	}
	
	
	
	
	
	
	private Logger log = LoggerFactory.getLogger(BSBMAlgebra.class);
	
	@Override
	public String processQuery(String queryShortname, String queryString) {
		

		String sql = mapper.rewrite(QueryFactory.create(queryString));
		
		sql = sql.replaceAll("WHERE", "\nWHERE").replaceAll("FROM", "\nFROM").replaceAll("SELECT", "\nSELECT").replaceAll("UNION", "\nUNION").replaceAll("JOIN", "\nJOIN").replaceAll("AND", "\nAND");
		log.info(sql);
		return sql;
		
	}
	
	@Override
	public void processQuery(String queryShortname, String query,
			String sqlQuery, ArrayList checkKeys) {
		processQuery(queryShortname, query);
	}

}
