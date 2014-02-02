package org.aksw.sparqlmap.core.mapper.translate;

import org.aksw.sparqlmap.core.ImplementationException;

import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpQuad;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadBlock;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;

public class QuadVisitorBase extends OpVisitorBase{
	
	@Override
	public void visit(OpBGP opBGP) {
		throw new ImplementationException("Move to quad");
	}
	
	
	@Override
	public void visit(OpGraph opGraph) {
		throw new ImplementationException("Move to quad");
	}
	
		
	@Override
	public void visit(OpQuadBlock quadBlock) {
		for(OpQuadPattern pattern : quadBlock.convert()){
			visit(pattern);
		}
	}
	
	
	
}
