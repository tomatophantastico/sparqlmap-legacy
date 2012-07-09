package org.aksw.sparqlmap.beautifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.r2rml.ColumnHelper;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVars;
import com.hp.hpl.jena.sparql.algebra.TransformCopy;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Datatype;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_LangMatches;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.util.VarUtils;


/**
 * In this case, more beautiful means that all expressions, that actually filter
 * are in filter clauses. This means, that ""
 * 
 * @author joerg
 * 
 */
public class SparqlBeautifier extends TransformCopy {

	private AlgebraGenerator agen = new  AlgebraGenerator();
	private int i = 0;

	Map<String,Node> termToVariable = new HashMap<String, Node>();
	@Override
	public Op transform(OpBGP opBGP) {
		List<Triple> patterns = opBGP.getPattern().getList();
		List<Triple> newPatterns = new ArrayList<Triple>();
		
		Map<String,String> var2Value = new HashMap<String, String>();
		ExprList exprList = new ExprList();

		for (Triple triple : patterns) {
			triple = uniquefyTriple(triple, exprList); 
			

			newPatterns.add(new Triple(
					rewriteNode(triple.getSubject(), exprList, termToVariable, var2Value), 
					rewriteNode(triple.getPredicate(), exprList, termToVariable, var2Value),
					rewriteNode(triple.getObject(), exprList, termToVariable, var2Value)));
		}

		OpBGP newOp = new OpBGP(BasicPattern.wrap(newPatterns));

		return OpFilter.filter(exprList, newOp);
	}

	
	/**
	 * guarantees that every variable is used only once in the pattern.
	 * @param triple
	 * @param exprList
	 * @return
	 */
	private Triple uniquefyTriple(Triple triple, ExprList exprList) {
		Node s = triple.getSubject();
		Node p = triple.getPredicate();
		Node o = triple.getObject();
		
		if(p.isVariable() && p.equals(s)){
			p = Var.alloc(i++ + org.aksw.sparqlmap.config.syntax.r2rml.ColumnHelper.COL_NAME_INTERNAL);
			exprList.add(new E_Equals(new ExprVar(s),new ExprVar(p)));
		}
		if (o.isVariable() && o.equals(s)) {
			o = Var.alloc(i++ + ColumnHelper.COL_NAME_INTERNAL);
			exprList.add(new E_Equals(new ExprVar(s),new ExprVar(o)));
		}
		if (o.isVariable() && o.equals(p)) {
			o = Var.alloc(i++ + ColumnHelper.COL_NAME_INTERNAL);
			exprList.add(new E_Equals(new ExprVar(p),new ExprVar(o)));
		}
		
		
		
		return new Triple(s, p, o);
	}



	private Node rewriteNode(Node n, ExprList addTo, Map<String,Node> termToVariable,Map<String,String> var2Value){
		
		if(n.isConcrete()){
			
			Node nNew = termToVariable.get(n.toString()); 
			if(nNew==null){
				nNew = Var.alloc(i++ + ColumnHelper.COL_NAME_INTERNAL);
				termToVariable.put(n.toString(), nNew);
			}
			

			if(! (var2Value.containsKey(nNew.getName())&&var2Value.get(nNew.getName()).equals(n.toString()))){
				var2Value.put(nNew.getName(), n.toString());
				
			

			
			
			Expr newExpr = null;
			if(n.isLiteral()){
				newExpr = 
						new E_Equals(new ExprVar(nNew),NodeValue.makeString(n.getLiteralValue().toString()));
				

				if(n.getLiteralDatatypeURI()!=null && !n.getLiteralDatatypeURI().isEmpty()){
					newExpr = 
						new E_Equals(
								new E_Datatype(new ExprVar(nNew)),
								NodeValue.makeNodeString(n.getLiteralDatatypeURI()));
				}
				if(n.getLiteralLanguage()!=null && !n.getLiteralLanguage().isEmpty()){
					newExpr = new E_LangMatches(new ExprVar(nNew), NodeValue.makeString(n.getLiteralLanguage()));
				}

			}else{
				//is uri
				newExpr  =new E_Equals(new ExprVar(nNew),NodeValue.makeNode(n));
			}
			addTo.add(newExpr);
			}	
			n = nNew;
		
		
		}
		
		return n;
	}
	
	@Override
	public Op transform(OpProject opProject, Op subOp) {
		
		OpVars.allVars(subOp);
		
		
		opProject.getVars();
		// TODO Auto-generated method stub
		return super.transform(opProject, subOp);
	}
	
	
	public Op compileToBeauty(Query sparql){
		
		Op query  = agen.compile(sparql);
		
		
		
		if(sparql.getProject().isEmpty()){
		
			sparql.setQueryResultStar(false);
			Set<Var> vars = new HashSet<Var>( OpVars.allVars(query));
			for (Var var : vars) {
				sparql.getProject().add(var);

			}
			query  = agen.compile(sparql);
		}
		
		
		
		
		Op newOp = Transformer.transform(this, query);
		
	
				
		return newOp;
	}
	
	
//	public static Query transform(Query sparql){
//		
//		Op op = new AlgebraGenerator().compile(sparql);
//
//		SparqlBeautifier qbeautify = new SparqlBeautifier();
//		
//		
//		Op newOp = Transformer.transform(qbeautify, op);
//		
//
//		
//		Query newSparql = OpAsQuery.asQuery(newOp);
//		
//		//we replace the * in the projection with all variables, that were not introduced by the rewriting 
//		
//		
//		
//		return QueryFactory.create(newSparql.toString());
//	}
	
	
	
	
	
	
	
	
	
	
}
