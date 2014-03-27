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

import org.aksw.sparqlmap.core.TranslationContext;
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
import org.aksw.sparqlmap.core.mapper.translate.FilterUtil;
import org.aksw.sparqlmap.core.mapper.translate.OptimizationConfiguration;
import org.aksw.sparqlmap.core.mapper.translate.QueryBuilderVisitor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
import com.hp.hpl.jena.sparql.algebra.AlgebraQuad;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadBlock;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;

@Service
public class AlgebraMapper implements Mapper {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AlgebraMapper.class);

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
	private FilterUtil filterUtil;
	
	@Autowired
	private OptimizationConfiguration fopt;
	
	private SparqlBeautifier beautifier = new SparqlBeautifier();
	
	public SparqlBeautifier getBeautifier() {
		return beautifier;
	}


	
	



	public void rewrite(TranslationContext context) {
		
		Query origQuery = context.getQuery();
		if(log.isDebugEnabled()){
			log.debug(origQuery.toString());
		}
		
		//first we beautify the Query


		context.setBeautifiedQuery( this.beautifier.compileToBeauty(origQuery)); // new  AlgebraGenerator().compile(beautified);
		
		
		if(log.isDebugEnabled()){
			log.debug(context.getBeautifiedQuery().toString());
		}

		context.setQueryInformation(FilterFinder.getQueryInformation(context.getBeautifiedQuery()));
		
		Binder binder = new Binder(this.mappingConf,context.getQueryInformation());
		
		context.setQueryBinding(binder.bind(context.getBeautifiedQuery()));
		if(log.isDebugEnabled()){
			log.debug(context.getQueryBinding().toString());
		}
		
		
		if(fopt.isOptimizeProjectPush()){
			context.getQueryInformation().setProjectionPushable( checkProjectionPush(origQuery, context.getQueryBinding()));
			
		}
		
		
		if(fopt.isOptimizeSelfUnion()){
			
		
			QueryDeunifier unionOpt = new QueryDeunifier(context.getQueryInformation(), context.getQueryBinding(),dth,exprconv,colhelp,fopt);
			if(!unionOpt.isFailed()){
			QueryInformation newqi = unionOpt.getQueryInformation();
			newqi.setProjectionPushable(context.getQueryInformation().isProjectionPush());
			context.setQueryInformation(newqi);
			context.setQueryBinding( unionOpt.getQueryBinding());
			}
		}
		
		QueryBuilderVisitor builderVisitor = new QueryBuilderVisitor(context,dth,exprconv,filterUtil);
		
		
		
		
		RightFirstWalker.walk(context.getQueryInformation().getQuery(), builderVisitor);
		
		
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
		
		context.setSqlQuery( out.toString());
		
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
				for (Quad quad : queryBinding.getBindingMap().keySet()) {
					if (quad.getSubject().equals(pvar)) {
						// now check all bindings
						for (TripleMap tripleMap : queryBinding.getBindingMap()
								.get(quad)) {
							if (!tripleMap.getSubject().isConstant()) {
								return false;
							}
						}
					}
					if (quad.getGraph().equals(pvar)) {
						// now check all bindings
						for (TripleMap tripleMap : queryBinding.getBindingMap()
								.get(quad)) {
							if (!tripleMap.getGraph().isConstant()) {
								return false;
							}
						}
					}
					if (quad.getPredicate().equals(pvar)) {
						for (TripleMap tripleMap : queryBinding.getBindingMap()
								.get(quad)) {
							for (PO po : tripleMap.getPos()) {
								if (!po.getPredicate().isConstant()) {
									return false;
								}
							}

						}
					}
					if (quad.getObject().equals(pvar)) {
						for (TripleMap tripleMap : queryBinding.getBindingMap()
								.get(quad)) {
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
	public List<TranslationContext> dump() {
		
		List<TranslationContext> contexts = new ArrayList<TranslationContext>();
		
//		Query spo = QueryFactory.create("SELECT ?s ?p ?o {{?s ?p ?o}}");
		Query spo = QueryFactory.create("SELECT ?g ?s ?p ?o {GRAPH ?g {?s ?p ?o}}");
		
		AlgebraGenerator gen = new AlgebraGenerator();
		Op qop = AlgebraQuad.quadize(gen.compile(spo));
		
		//Triple triple = ((OpBGP)((OpGraph)((OpProject)qop).getSubOp()).getSubOp()).getPattern().get(0);
		Quad triple = ((OpQuadPattern)(((OpProject)qop)).getSubOp()).getPattern().get(0);
		
		
		
		
		
		for(TripleMap trm: mappingConf.getTripleMaps()){
			TranslationContext context = new TranslationContext();
			context.setQuery(spo);
			context.setQueryName("dump query");

			Map<Quad,Collection<TripleMap>> bindingMap = new HashMap<Quad, Collection<TripleMap>>();
			bindingMap.put(triple, Arrays.asList(trm));
			
			
			MappingBinding qbind = new MappingBinding(bindingMap);
			context.setQueryBinding(qbind);

			
			QueryInformation qi = FilterFinder.getQueryInformation(qop);
			qi.setProject((OpProject)qop);

			context.setQueryInformation(qi);
			
			QueryBuilderVisitor qbv = new QueryBuilderVisitor(context,dth,exprconv,filterUtil);
			
			
			OpWalker.walk(qop, qbv);
			
			// prepare deparse select
			StringBuilder sbsql = new StringBuilder();
			Select select = qbv.getSqlQuery();
			SelectDeParser selectDeParser  = dbconf.getSelectDeParser(sbsql);
			
      if (fopt.isOptimizeSelfUnion()) {

        QueryDeunifier unionOpt =
          new QueryDeunifier(context.getQueryInformation(),
            context.getQueryBinding(), dth, exprconv, colhelp, fopt);
        if (!unionOpt.isFailed()) {
          QueryInformation newqi = unionOpt.getQueryInformation();
          newqi.setProjectionPushable(context.getQueryInformation()
            .isProjectionPush());
          context.setQueryInformation(newqi);
          context.setQueryBinding(unionOpt.getQueryBinding());
        }
      }

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
			
			
			
			context.setSqlQuery(sbsql.toString());
			contexts.add(context);
			
			
		}
		
		
		
		return contexts;
	}


}
