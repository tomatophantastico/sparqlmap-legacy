package org.aksw.sparqlmap.core.mapper;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.util.BaseSelectVisitor;

import org.aksw.sparqlmap.core.config.syntax.r2rml.ColumnHelper;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.core.mapper.finder.FilterFinder;
import org.aksw.sparqlmap.core.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.core.mapper.finder.QueryInformation;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.ExpressionConverter;
import org.aksw.sparqlmap.core.mapper.translate.OptimizationConfiguration;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.TransformBase;
import com.hp.hpl.jena.sparql.algebra.TransformCopy;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadBlock;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprList;


public class QueryDeunifier extends TransformCopy{

	private Multimap<String, String> var2varname = HashMultimap.create();

	QueryInformation qi;
	MappingBinding queryBinding;
	DataTypeHelper dth;
	ExpressionConverter exprconv;
	ColumnHelper colhelp;
	OptimizationConfiguration fopt;
	Map<Quad, Collection<TripleMap>> newbindingMap = new HashMap<Quad, Collection<TripleMap>>();
	Op query;
	public QueryDeunifier(QueryInformation qi,
			MappingBinding queryBinding, DataTypeHelper dth,
			ExpressionConverter exprconv, ColumnHelper colhelp,
			OptimizationConfiguration fopt) {
		this.queryBinding = queryBinding;
		this.qi =qi;
		this.dth = dth;
		this.exprconv = exprconv;
		this.colhelp = colhelp;
		this.fopt = fopt;
		
		
		
		query = Transformer.transform( this,qi.getQuery());
		
		
	}

	
	
	@Override
	public Op transform(OpFilter opFilter, Op subOp) {
//		
//		for (opFilter.getExprs().getList())
//		
		
		
		return super.transform(opFilter, subOp);
	}
	
	
	@Override
	public Op transform(OpQuadPattern opBGP) {
		//only applicable if the triple patterns have multiple bindings.
		
		
		
		
		boolean merge = false;
		if( opBGP.getPattern().getList().size()==1){
			Quad triple = opBGP.getPattern().getList().iterator().next();
			for(TripleMap tm : queryBinding.getBindingMap().get(triple)){
					if(tm.getPos().size()>1){
						//yes we can merge them
						merge = true;
						
					}
			}
		}
		
		//as we can, we do it now
		if(merge){
			Quad triple = opBGP.getPattern().getList().iterator().next();
			Set<Op> unionops = new HashSet<Op>(); 
			
			
			for(TripleMap tm : queryBinding.getBindingMap().get(triple)){
				Set<Quad>  lefjointriples = new HashSet<Quad>();
				int i = 0;
				for(PO po: tm.getPos()){
					String s = triple.getSubject().getName();
					String p = triple.getPredicate().getName() +"-du" + String.format("%02d", i);  
					var2varname.put(triple.getPredicate().getName(), p);
					String o = triple.getObject().getName()+"-du" + String.format("%02d", i);  
					var2varname.put(triple.getObject().getName(), o);
					
					Quad newTriple = new Quad(triple.getGraph(), triple.getSubject(),Var.alloc(p),Var.alloc(o));
					
					TripleMap newTripleMap = tm.getShallowCopy();
					newTripleMap.getPos().retainAll(Arrays.asList(po));
					
					newbindingMap.put(newTriple, Arrays.asList(newTripleMap));
					lefjointriples.add(newTriple);
					i++;
				}
//				if(lefjointriples.size()==1){
//					BasicPattern bp = new BasicPattern();
//					bp.add(lefjointriples.iterator().next());
//					OpBGP bgp = new OpBGP();
//					
//					unionops.add(bgp);
//				}else{
				
				Iterator<Quad> itrip = lefjointriples.iterator();
				
				OpQuadBlock oqb = new OpQuadBlock();
				
				oqb.getPattern().add(itrip.next());
				

				OpLeftJoin oplj = (OpLeftJoin) OpLeftJoin.create(OpTable.unit(),oqb
						, (Expr) null);

				while (itrip.hasNext()) {
					
					OpQuadBlock oqb2 = new OpQuadBlock();
					oqb2.getPattern().add(itrip.next());
					oplj = (OpLeftJoin) OpLeftJoin.create( oplj,oqb2, (Expr) null);
					

				}
				unionops.add(oplj);
//				}
			}
			if(unionops.size()==1){
				return unionops.iterator().next();
			}else{
				Iterator<Op> iop = unionops.iterator();

				OpUnion union = new OpUnion(iop.next(), iop.next());
				
				while(iop.hasNext()){
					union = new OpUnion(opBGP, iop.next());
				}
				return union;
			}
			
		}else{
			return super.transform(opBGP);
		}
		
		
		
		
		
		
	}
	
	
	public MappingBinding getQueryBinding() {
		
		queryBinding.getBindingMap().putAll(newbindingMap);
		return queryBinding;
	}


	public boolean isFailed() {
		return false;
	}


	public QueryInformation getQueryInformation() {
		
		return  FilterFinder.getQueryInformation(query);
	}
	
	

}
