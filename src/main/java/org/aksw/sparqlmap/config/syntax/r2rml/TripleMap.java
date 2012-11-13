package org.aksw.sparqlmap.config.syntax.r2rml;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.ImplementationException;

public class TripleMap {
	
	static int nameCounter = 1;
	
	String uri;
	public FromItem from;
	TermMap subject;
	Set<PO> pos = new LinkedHashSet<PO>();
	
	
	
	
	public TripleMap(String uri, FromItem from) {
		super();
		this.uri = uri;
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
		PO po = new PO();
		po.setPredicate(predicate);
		po.setObject(object);
		pos.add(po);
	}
	
	public Set<PO> getPos() {
		return pos;
	}


	public class PO{
		private TermMap predicate;
		private TermMap object;
		
		
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
		
	}
	
	
	public void toTtl(StringBuffer ttl){
		ttl.append("<" +this.uri+ ">");
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
		TripleMap copy = new TripleMap(this.uri,this.from);
		copy.pos = new HashSet<TripleMap.PO>(pos);
		copy.subject = subject;
		return copy;		
	}
	
	@Override
	public String toString() {
		
		return "TripleMap for " + this.from.toString();
	}
	
	

}
