package org.aksw.sparqlmap.mapper;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.RDB2RDF.ReturnType;
import org.aksw.sparqlmap.beautifier.SparqlBeautifier;
import org.aksw.sparqlmap.config.syntax.DBConnectionConfiguration;
import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.QueryBuilderVisitor;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml.Binding;
import org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml.MappingFilterFinder;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;

public class AlgebraBasedMapper implements Mapper {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AlgebraBasedMapper.class);

	private R2RMLModel mappingConf;

	private DBConnectionConfiguration dbconf;
	
	private SparqlBeautifier beautifier = new SparqlBeautifier();
	
	public SparqlBeautifier getBeautifier() {
		return beautifier;
	}


	
	public AlgebraBasedMapper(R2RMLModel mappingConf, DBConnectionConfiguration dbconf){
		this.mappingConf = mappingConf;
		this.dbconf = dbconf;
	}




	public String rewrite(Query sparql) {
		
		Query origQuery = sparql;

		log.debug(origQuery.toString());
		//first we beautify the Query


		Op op = this.beautifier.compileToBeauty(origQuery); // new  AlgebraGenerator().compile(beautified);
		log.debug(op.toString());
		
		MappingFilterFinder mff = new MappingFilterFinder(mappingConf);
		
		Binding queryBinding = mff.createBindnings(op);
		

		
		QueryBuilderVisitor builderVisitor = new QueryBuilderVisitor(mappingConf, mff,queryBinding,dbconf.getDataTypeHelper(), new FilterUtil(dbconf.getDataTypeHelper(), mappingConf));
		
		
		OpWalker.walk(op, builderVisitor);
		
		
		// prepare deparse select
		StringBuffer out = new StringBuffer();
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
		
		Query spo = QueryFactory.create("SELECT ?s ?p ?o {?s ?p ?o}");
		
		AlgebraGenerator gen = new AlgebraGenerator();
		Op qop = gen.compile(spo);
		
		Triple triple = ((OpBGP)((OpProject)qop).getSubOp()).getPattern().get(0);
		
		
		
		
		
		
		for(TripleMap trm: mappingConf.getTripleMaps()){
			Set<Triple> triples = new HashSet<Triple>();
			Set<TripleMap> tripleMaps = new HashSet<TripleMap>();
			tripleMaps.add(trm);
			triples.add(triple);
			Binding qbind = new Binding(mappingConf,triples);
			qbind.getBinding().replaceValues(triple, tripleMaps);
			MappingFilterFinder mff = new MappingFilterFinder(mappingConf);
			mff.setProject((OpProject)qop);
			
			QueryBuilderVisitor qbv = new QueryBuilderVisitor(mappingConf, mff, qbind,dbconf.getDataTypeHelper(), new FilterUtil(dbconf.getDataTypeHelper(), mappingConf));
			
			
			OpWalker.walk(qop, qbv);
			
			// prepare deparse select
			StringBuffer sbsql = new StringBuffer();
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
