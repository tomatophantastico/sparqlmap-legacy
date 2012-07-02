package org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder;

import static org.junit.Assert.*;

import org.junit.Test;


	import static org.junit.Assert.*;

	import java.io.InputStreamReader;
	import java.util.Set;

import org.aksw.sparqlmap.BSBMAlgebra;
import org.aksw.sparqlmap.BSBMTest;
import org.aksw.sparqlmap.BaseTest;
import org.aksw.sparqlmap.beautifier.SparqlBeautifier;
import org.aksw.sparqlmap.config.syntax.MappingConfiguration;
import org.aksw.sparqlmap.config.syntax.SparqlMapConfiguration;
import org.aksw.sparqlmap.config.syntax.SimpleConfigParser;
import org.aksw.sparqlmap.db.SQLAccessFacade;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.AlgebraBasedMapper;
	import org.apache.log4j.spi.LoggerFactory;
	import org.junit.Before;
	import org.junit.Test;
	import org.slf4j.Logger;

	import com.hp.hpl.jena.query.QueryFactory;
	import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
	import com.hp.hpl.jena.sparql.algebra.Op;
	import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
	import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
	import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
	import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;

	public class MappingFilterFinderTest extends BaseTest{
		static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(MappingFilterFinderTest.class);
		private SparqlBeautifier beauty =  new SparqlBeautifier();
		
		
		
		
		
//		@Before
//		public void initMapper() throws Throwable {
//			beauty = new SparqlBeautifier();
//
//			SimpleConfigParser parser = new SimpleConfigParser();
//
//			config = parser.parse(new InputStreamReader(
//					ClassLoader.getSystemResourceAsStream("bsbm.r2rml")));
//			
//			mconfig = new MappingConfiguration(config);
//
//			mapper = new AlgebraBasedMapper(config);
//
//			db = new SQLAccessFacade(config.getDbConn());
//
//			mapper.setConn(db);
//			
//			
//
//		}
		Op leftsideOfUnionBGP;
		
		

		@Test
		public void test() {
			String query  = BSBMTest.q11;
			SparqlBeautifier beauty = new SparqlBeautifier();
			
			Op queryOp =  beauty.compileToBeauty(QueryFactory.create(query));
			
			OpWalker.walk(queryOp, new OpVisitorBase(){
				@Override
				public void visit(OpUnion opUnion) {
					leftsideOfUnionBGP = opUnion.getLeft();
				}
				
			});

			MappingFilterFinder finder = new MappingFilterFinder(this.config.getMappingConfiguration(), queryOp);
			log.info(finder.toString());
			
			Set<Expr> exprs = finder.getFilterForVariables(leftsideOfUnionBGP, Var.alloc("s"), Var.alloc("yxf"));
			for (Expr expr : exprs) {
				log.info("expr:" + expr.toString());
			}
			log.info(finder.toString());
		}
		
		
		
		OpLeftJoin optional;
		@Test
		public void testOpt(){
			String query = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
					+ "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
					+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
					+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
					+ "SELECT "
					+ "?label "
					+ "WHERE {  "
					+ "?prod bsbm:productFeature ?f . 	"
					+ "OPTIONAL {?prod rdfs:label ?label . }	"
					+ "}";
			Op queryOp =  beauty.compileToBeauty(QueryFactory.create(query));
			
			
			log.info(queryOp.toString());
			OpWalker.walk(queryOp, new OpVisitorBase(){
				@Override
				public void visit(OpLeftJoin opLeftJoin) {
					optional = (OpLeftJoin) opLeftJoin;
				}
				
			});
			
			
			Op right = optional.getRight();
			Op left = optional.getLeft();
			
			MappingFilterFinder finder  = new MappingFilterFinder(this.config.getMappingConfiguration(), queryOp);
			ScopeBlock sr = finder.getScopeBlock(right);
			ScopeBlock sl = finder.getScopeBlock(left);
			
			
			int i = 0;
		}
		
		
		@Override
		public String processQuery(String shortname, String query) {
			return null;
			// TODO Auto-generated method stub
			
		}
		

	}
