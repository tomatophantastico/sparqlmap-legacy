package org.aksw.sparqlmap.config.syntax.r2rml;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.mapper.translate.ImplementationException;

public class TripleMap {
	
	static int nameCounter = 1;
	
	private String uri;
	public FromItem from;
	TermMap subject;
	Set<PO> pos = new LinkedHashSet<PO>();
	
	
	
	
	public TripleMap(String uri, FromItem from) {
		super();
		this.setUri(uri);
		this.from = from;
	}
	
//	public TripleMap(FromItem from) {
//		super();
//		this.from = from;
//		if(from instanceof Table){
//			name = "TripleMap_" + ((Table)from).getName();
//		}else{
//			name = "TripleMap_" + nameCounter++;
//		}
//	}


	
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
	
	
	public void toTtl(StringBuffer ttl){
		ttl.append("<" +this.getUri()+ ">");
		if(from instanceof Table){
			ttl.append("rr:logicalTable [ rr:tableName \""+((Table)from).getName()+"\" ];\n");
		}if(from instanceof SubSelect){
			ttl.append("rr:sqlQuery \"\"\"" +((SubSelect)from).getSelectBody().toString() +"\"\"\"\n");
		}else{
			throw new ImplementationException("Encountered unmappable FromItem ");
		}
		ttl.append("rr:subjectMap [ \n");
		subject.toTtl(ttl);
		ttl.append("\n];\n");
		
		for (PO po : this.pos) {
			ttl.append("rr:predicateObjectMap [\n");
			po.predicate.toTtl(ttl);
			ttl.append("\n");
			po.object.toTtl(ttl);
			ttl.append("\n];\n");
			
		}
		
		
	}
	
	public TermMap getSubject() {
		return subject;
	}
	
	public TripleMap getShallowCopy(){
		TripleMap copy = new TripleMap(this.getUri(),this.from);
		copy.pos = new HashSet<TripleMap.PO>(pos);
		copy.subject = subject;
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
