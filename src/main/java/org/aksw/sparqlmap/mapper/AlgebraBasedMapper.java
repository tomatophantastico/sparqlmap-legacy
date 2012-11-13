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
import org.aksw.sparqlmap.config.syntax.IDBAccess;
import org.aksw.sparqlmap.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ExpressionConverter;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.QueryBuilderVisitor;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml.MappingBinding;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml.MappingFilterFinder;
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
	
	private SparqlBeautifier beautifier = new SparqlBeautifier();
	
	public SparqlBeautifier getBeautifier() {
		return beautifier;
	}


	
	



	public String rewrite(Query sparql) {
		
		Query origQuery = sparql;

		log.debug(origQuery.toString());
		//first we beautify the Query


		Op op = this.beautifier.compileToBeauty(origQuery); // new  AlgebraGenerator().compile(beautified);
		log.debug(op.toString());
		
		MappingFilterFinder mff = new MappingFilterFinder(mappingConf);
		
		MappingBinding queryBinding = mff.createBindnings(op);
		

		
		QueryBuilderVisitor builderVisitor = new QueryBuilderVisitor(mff,queryBinding,dth,exprconv,colhelp);
		
		
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
			qbind.getBinding().replaceValues(triple, tripleMaps);
			MappingFilterFinder mff = new MappingFilterFinder(mappingConf);
			mff.setProject((OpProject)qop);
			
			QueryBuilderVisitor qbv = new QueryBuilderVisitor(mff,qbind,dth,exprconv,colhelp);
			
			
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
