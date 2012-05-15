package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import static org.junit.Assert.*;

import java.io.InputStreamReader;
import java.util.Set;

import org.aksw.sparqlmap.BSBMAlgebra;
import org.aksw.sparqlmap.config.syntax.MappingConfiguration;
import org.aksw.sparqlmap.config.syntax.R2RConfiguration;
import org.aksw.sparqlmap.config.syntax.SimpleConfigParser;
import org.aksw.sparqlmap.db.SQLAccessFacade;
import org.apache.log4j.spi.LoggerFactory;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;

public class FilterFinderTest {
	static Logger log = org.slf4j.LoggerFactory.getLogger(FilterFinderTest.class);
	private SQLAccessFacade db;
	private AlgebraBasedMapper mapper;
	private R2RConfiguration config;
	private MappingConfiguration mconfig;
	
	
	
	
	
	@Before
	public void initMapper() throws Throwable {

		SimpleConfigParser parser = new SimpleConfigParser();

		config = parser.parse(new InputStreamReader(
				ClassLoader.getSystemResourceAsStream("bsbm.r2rml")));
		
		mconfig = new MappingConfiguration(config);

		mapper = new AlgebraBasedMapper(config);

		db = new SQLAccessFacade(config.getDbConn());

		mapper.setConn(db);
		
		

	}
	Op leftsideOfUnionBGP;
	
	

	@Test
	public void test() {
		String query  = "select distinct ?s ?p where {?s ?p ?o Filter (?s = <uri://asdfp>) FILTER (?yxf = 'stuff') OPTIONAL {{?s ?p2 ?yxf  FILTER (?p2 = <uri://yxf>)}UNION {?s ?p3 ?yxf FILTER (?p3 = <uri://yxf2>)}}} ORDER BY ?s LIMIT 1 offset 10";
		
		Op queryOp  = new AlgebraGenerator().compile(QueryFactory.create(query));
		
		
		
		
		OpWalker.walk(queryOp, new OpVisitorBase(){
			@Override
			public void visit(OpUnion opUnion) {
				leftsideOfUnionBGP = opUnion.getLeft();
			}
			
		});
		
				
		
		FilterFinder finder = new FilterFinder(mconfig, queryOp);
		
		Set<Expr> exprs = finder.getFilterForVariables(leftsideOfUnionBGP, Var.alloc("s"), Var.alloc("yxf"));
		for (Expr expr : exprs) {
			log.debug("expr:" + expr.toString());
		}
		log.debug(finder.toString());
	}

}
