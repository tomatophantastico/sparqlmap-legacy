package org.aksw.sparqlmap.core.config.syntax.r2rml;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.core.ImplementationException;

public class TripleMap {
	
	static int nameCounter = 1;
	
	private String uri;
	public FromItem from;
	private TermMap subject;
	private Set<PO> pos = new LinkedHashSet<PO>();
	private TermMap graph;
	
	
	
	public TripleMap(String uri, FromItem from) {
		super();
		this.setUri(uri);
		this.from = from;
	}
	

	
	public void addPO(TermMap predicate,TermMap object){
		PO po = new PO(this);
		po.setPredicate(predicate);
		po.setObject(object);
		pos.add(po);
	}
	
	public Set<PO> getPos() {
		return pos;
	}


	public class PO{
		
		private TripleMap tripleMap;
		private TermMap predicate;
		private TermMap object;
		
		
		
		public PO(TripleMap tripleMap) {
			super();
			this.tripleMap = tripleMap;
		}
		public TripleMap getTripleMap() {
			return tripleMap;
		}
		public TermMap getPredicate() {
			return predicate;
		}
		public void setPredicate(TermMap predicate) {
			this.predicate = predicate;
		}
		public TermMap getObject() {
			return object;
		}
		public void setObject(TermMap object) {
			this.object = object;
		}
		
		@Override
		public String toString() {
			
			return predicate.toString() + " " + object.toString();
		}
		
	}
	
	public FromItem getFrom() {
		return from;
	}
	
	public TermMap getGraph() {
		return graph;
	}
	
	public void setGraph(TermMap graph) {
		this.graph = graph;
	}
	
	public void setFrom(FromItem from) {
		this.from = from;
	}
	
		
	public TermMap getSubject() {
		return subject;
	}
	public void setSubject(TermMap subject) {
		this.subject = subject;
	}
	
	public TripleMap getShallowCopy(){
		TripleMap copy = new TripleMap(this.getUri(),this.from);
		copy.setGraph(graph);
		copy.pos = new HashSet<TripleMap.PO>(pos);
		copy.subject = subject;
		return copy;		
	}
	
	public TripleMap getDeepCopy(){
		TripleMap copy = new TripleMap(this.getUri(), this.from);
		copy.setGraph(graph);
		copy.subject = subject.clone("");
		for(PO po:pos){
			copy.addPO(po.getObject().clone(""), po.getPredicate().clone(""));
		}
		
		return copy;
	}
	
	@Override
	public String toString() {
		
		return "TripleMap: " + uri;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}
	
	

}
