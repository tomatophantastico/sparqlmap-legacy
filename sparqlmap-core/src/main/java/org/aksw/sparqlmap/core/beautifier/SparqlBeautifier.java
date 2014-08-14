package org.aksw.sparqlmap.core.beautifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
import com.hp.hpl.jena.sparql.algebra.AlgebraQuad;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVars;
import com.hp.hpl.jena.sparql.algebra.TransformCopy;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpQuad;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadBlock;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.QuadPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Datatype;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_LangMatches;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;


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
	
	Logger log = LoggerFactory.getLogger(SparqlBeautifier.class); 
	

	Map<String,Node> termToVariable = new HashMap<String, Node>();
	
	
	@Override
	public Op transform(OpQuadPattern quadBlock) {
		List<Quad> patterns = quadBlock.getPattern().getList();
		OpQuadBlock newOp = new OpQuadBlock();
		
		Map<String,String> var2Value = new HashMap<String, String>();
		ExprList exprList = new ExprList();

		for (Quad quad : patterns) {
			quad = uniquefyTriple(quad, exprList); 
			

			newOp.getPattern().add(new Quad(
					rewriteNode(quad.getGraph(), exprList, termToVariable, var2Value),
					rewriteNode(quad.getSubject(), exprList, termToVariable, var2Value), 
					rewriteNode(quad.getPredicate(), exprList, termToVariable, var2Value),
					rewriteNode(quad.getObject(), exprList, termToVariable, var2Value)));
		}

		
		
		Op op = OpFilter.filter(exprList, newOp);

		return op;
	}
	
	@Override
	public Op transform(OpFilter opFilter, Op subOp) {

		return opFilter.copy(subOp);
	}

	
	/**
	 * Creates a new quad out of the old one, such that
	 * every variable is used only once in the pattern.
	 * 
	 * for example {?x ?x ?y} -> {?x ?genvar_1 ?y. FILTER (?x = ?genvar_1)}
	 * 
	 * @param quad
	 * @param exprList the list with the equals conditions
	 * @return the rewritten Quad
	 */
	private Quad uniquefyTriple(Quad quad, ExprList exprList) {
		
		List<Node> quadNodes = Arrays.asList(quad.getGraph(),quad.getSubject(),quad.getPredicate(),quad.getObject());
		
		List<Node> uniqeNodes = new ArrayList<Node>();
		
		for(Node quadNode: quadNodes){
			if(quadNode.isVariable()&&uniqeNodes.contains(quadNode)){
				Var var_new = Var.alloc(i++ + ColumnHelper.COL_NAME_INTERNAL);
				uniqeNodes.add(var_new);
				exprList.add(new E_Equals(new ExprVar(quadNode),new ExprVar(var_new)));

			}else{
				uniqeNodes.add(quadNode);
			}
		}
		
		return new Quad(uniqeNodes.remove(0), uniqeNodes.remove(0),uniqeNodes.remove(0),uniqeNodes.remove(0));
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
					// no it is not, create the equivalnce check
					newExpr = new E_Equals(new ExprVar(nNew),
							NodeValue.makeNode(n));

				}
				addTo.add(newExpr);
			}	
			n = nNew;
		}
		return n;
	}
	
	@Override
	public Op transform(OpProject opProject, Op subOp) {
		
		
		
		OpVars.mentionedVars(subOp);
		
		
		opProject.getVars();
		// TODO Auto-generated method stub
		return super.transform(opProject, subOp);
	}
	
	
	
	/**
	 * Transforms the query into the form required for sparqlmap.
	 * Includes filter extraction and rewriting some patterns. 
	 * 
	 * @param sparql
	 * @return
	 */
	
	public Op compileToBeauty(Query sparql){
		
		Op query  = agen.compile(sparql);
		
		
		
		
		// this odd construct is neccessary as there seems to be no convenient way of extracting the vars of a query from the non-algebra-version.
		if(sparql.getProject().isEmpty()){
		
			sparql.setQueryResultStar(false);
			Set<Var> vars = new HashSet<Var>( OpVars.mentionedVars(query));
			for (Var var : vars) {
				sparql.getProject().add(var);

			}
			query  = agen.compile(sparql);
		}
		
		query = AlgebraQuad.quadize(query);		
		Op newOp = Transformer.transform(this, query);
		
		
			
		return newOp;
	}
	
	
	
	
}
