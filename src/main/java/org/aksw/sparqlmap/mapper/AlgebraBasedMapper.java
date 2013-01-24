package org.aksw.sparqlmap.mapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.beautifier.SparqlBeautifier;
import org.aksw.sparqlmap.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.db.IDBAccess;
import org.aksw.sparqlmap.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.mapper.finder.MappingFilterFinder;
import org.aksw.sparqlmap.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.mapper.translate.ExpressionConverter;
import org.aksw.sparqlmap.mapper.translate.FilterOptimizer;
import org.aksw.sparqlmap.mapper.translate.QueryBuilderVisitor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;

@Service
public class AlgebraBasedMapper implements Mapper {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AlgebraBasedMapper.class);

	@Autowired
	private R2RMLModel mappingConf;

	@Autowired
	private IDBAccess dbconf;
	
	@Autowired
	private DataTypeHelper dth;
	
	@Autowired
	private ColumnHelper colhelp;
	
	@Autowired 
	private ExpressionConverter exprconv;
	
	@Autowired
	private FilterOptimizer fopt;
	
	private SparqlBeautifier beautifier = new SparqlBeautifier();
	
	public SparqlBeautifier getBeautifier() {
		return beautifier;
	}


	
	



	public String rewrite(Query sparql) {
		
		Query origQuery = sparql;

		log.info(origQuery.toString());
		//first we beautify the Query


		Op op = this.beautifier.compileToBeauty(origQuery); // new  AlgebraGenerator().compile(beautified);
		log.info(op.toString());
		
		MappingFilterFinder mff = new MappingFilterFinder(mappingConf);
		
		MappingBinding queryBinding = mff.createBindnings(op);
		
		log.info(queryBinding.toString());
		
		QueryBuilderVisitor builderVisitor = new QueryBuilderVisitor(mff,queryBinding,dth,exprconv,colhelp,fopt);
		
		OpWalker.walk(op, builderVisitor);
		
		
		// prepare deparse select
		StringBuilder out = new StringBuilder();
		Select select = builderVisitor.getSqlQuery();
		SelectDeParser selectDeParser  = dbconf.getSelectDeParser(out);
		
		selectDeParser.setBuffer(out);
//		ExpressionDeParser expressionDeParser =  mappingConf.getR2rconf().getDbConn().getExpressionDeParser(selectDeParser, out);
//		selectDeParser.setExpressionVisitor(expressionDeParser);
		if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
			out.append("WITH ");
			for (Iterator iter = select.getWithItemsList().iterator(); iter.hasNext();) {
				WithItem withItem = (WithItem)iter.next();
				out.append(withItem);
				if (iter.hasNext())
					out.append(",");
				out.append(" ");
			}
		}
		select.getSelectBody().accept(selectDeParser);
		
		
		
		String sqlResult = out.toString();
		log.debug(sqlResult);
		return sqlResult;
	}
	
	
	
	@Override
	public List<String> dump() {
		
		List<String> queries = new ArrayList<String>();
		
		Query spo = QueryFactory.create("SELECT ?g ?s ?p ?o {GRAPH ?g {?s ?p ?o}}");
		
		AlgebraGenerator gen = new AlgebraGenerator();
		Op qop = gen.compile(spo);
		
		Triple triple = ((OpBGP)((OpGraph)((OpProject)qop).getSubOp()).getSubOp()).getPattern().get(0);
		
		
		
		
		
		
		for(TripleMap trm: mappingConf.getTripleMaps()){
			Set<Triple> triples = new HashSet<Triple>();
			Set<TripleMap> tripleMaps = new HashSet<TripleMap>();
			tripleMaps.add(trm);
			triples.add(triple);
			MappingBinding qbind = new MappingBinding(mappingConf,triples);
			
			qbind.getBinding().put(triple, tripleMaps);
			MappingFilterFinder mff = new MappingFilterFinder(mappingConf);
			mff.setProject((OpProject)qop);
			
			QueryBuilderVisitor qbv = new QueryBuilderVisitor(mff,qbind,dth,exprconv,colhelp,fopt);
			
			
			OpWalker.walk(qop, qbv);
			
			// prepare deparse select
			StringBuilder sbsql = new StringBuilder();
			Select select = qbv.getSqlQuery();
			SelectDeParser selectDeParser  = dbconf.getSelectDeParser(sbsql);
			
			selectDeParser.setBuffer(sbsql);
//			ExpressionDeParser expressionDeParser =  mappingConf.getR2rconf().getDbConn().getExpressionDeParser(selectDeParser, out);
//			selectDeParser.setExpressionVisitor(expressionDeParser);
			if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
				sbsql.append("WITH ");
				for (Iterator iter = select.getWithItemsList().iterator(); iter.hasNext();) {
					WithItem withItem = (WithItem)iter.next();
					sbsql.append(withItem);
					if (iter.hasNext())
						sbsql.append(",");
					sbsql.append(" ");
				}
			}
			select.getSelectBody().accept(selectDeParser);
			
			
			
			queries.add( sbsql.toString());
			
			
		}
		
		
		
		return queries;
	}

}
