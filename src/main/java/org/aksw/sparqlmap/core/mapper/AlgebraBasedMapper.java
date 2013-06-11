package org.aksw.sparqlmap.core.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.core.beautifier.SparqlBeautifier;
import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.finder.Binder;
import org.aksw.sparqlmap.core.mapper.finder.FilterFinder;
import org.aksw.sparqlmap.core.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.core.mapper.finder.QueryInformation;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.ExpressionConverter;
import org.aksw.sparqlmap.core.mapper.translate.FilterOptimizer;
import org.aksw.sparqlmap.core.mapper.translate.QueryBuilderVisitor;
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
import com.hp.hpl.jena.sparql.core.Var;

@Service
public class AlgebraBasedMapper implements Mapper {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AlgebraBasedMapper.class);

	@Autowired
	private R2RMLModel mappingConf;

	@Autowired
	private DBAccess dbconf;
	
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
		if(log.isDebugEnabled()){
			log.debug(origQuery.toString());
		}
		
		//first we beautify the Query


		Op op = this.beautifier.compileToBeauty(origQuery); // new  AlgebraGenerator().compile(beautified);
		if(log.isDebugEnabled()){
			log.debug(op.toString());
		}
		
		
		QueryInformation qi = FilterFinder.getQueryInformation(op);
		
		Binder binder = new Binder(this.mappingConf,qi);
		
		MappingBinding queryBinding = binder.bind(op);
		if(log.isDebugEnabled()){
			log.debug(queryBinding.toString());
		}
		
		
		if(fopt.isOptimizeProjectPush()){
			qi.setProjectionPushable( checkProjectionPush(origQuery, queryBinding));
			
		}
		
		
		if(fopt.isOptimizeSelfUnion()){
			
		
			QueryDeunifier unionOpt = new QueryDeunifier(qi,queryBinding,dth,exprconv,colhelp,fopt);
			if(!unionOpt.isFailed()){
			QueryInformation newqi = unionOpt.getQueryInformation();
			newqi.setProjectionPushable(qi.isProjectionPush());
			qi = newqi;
			queryBinding = unionOpt.getQueryBinding();
			}
		}
		
		QueryBuilderVisitor builderVisitor = new QueryBuilderVisitor(qi,queryBinding,dth,exprconv,colhelp,fopt);
		
		
		
		
		RightFirstWalker.walk(qi.getQuery(), builderVisitor);
		
		
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
	
		return sqlResult;
	}







	private boolean checkProjectionPush(Query origQuery,
			MappingBinding queryBinding) {
		// check here if we can projection push
		if (!origQuery.isDistinct()) {
			return false;
		}
			// check if all bindings for projected variables are constant
			List<Var> pvars = origQuery.getProjectVars();

			for (Var pvar : pvars) {
				for (Triple triple : queryBinding.getBindingMap().keySet()) {
					if (triple.getSubject().equals(pvar)) {
						// now check all bindings
						for (TripleMap tripleMap : queryBinding.getBindingMap()
								.get(triple)) {
							if (!tripleMap.getSubject().isConstant()) {
								return false;
							}
						}
					}
					if (triple.getPredicate().equals(pvar)) {
						for (TripleMap tripleMap : queryBinding.getBindingMap()
								.get(triple)) {
							for (PO po : tripleMap.getPos()) {
								if (!po.getPredicate().isConstant()) {
									return false;
								}
							}

						}
					}
					if (triple.getObject().equals(pvar)) {
						for (TripleMap tripleMap : queryBinding.getBindingMap()
								.get(triple)) {
							for (PO po : tripleMap.getPos()) {
								if (!po.getObject().isConstant()) {
									return false;
								}
							}

						}
					}
				}
			}

		return true;
	}
	
	
	
	@Override
	public List<String> dump() {
		
		List<String> queries = new ArrayList<String>();
		
		Query spo = QueryFactory.create("SELECT ?g ?s ?p ?o {GRAPH ?g {?s ?p ?o}}");
		
		AlgebraGenerator gen = new AlgebraGenerator();
		Op qop = gen.compile(spo);
		
		Triple triple = ((OpBGP)((OpGraph)((OpProject)qop).getSubOp()).getSubOp()).getPattern().get(0);
		
		
		
		
		
		
		for(TripleMap trm: mappingConf.getTripleMaps()){
			Map<Triple,Collection<TripleMap>> bindingMap = new HashMap<Triple, Collection<TripleMap>>();
			bindingMap.put(triple, Arrays.asList(trm));
			
			
			MappingBinding qbind = new MappingBinding(bindingMap);
			
			
			QueryInformation mff = FilterFinder.getQueryInformation(qop);
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
