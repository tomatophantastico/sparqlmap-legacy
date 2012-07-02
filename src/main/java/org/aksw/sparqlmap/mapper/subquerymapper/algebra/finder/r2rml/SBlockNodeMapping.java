package org.aksw.sparqlmap.mapper.subquerymapper.algebra.finder.r2rml;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.query.Mapping;

/**
 * this class contains all the information to what mappings a node will map.
 * 
 * @author joerg
 * 
 */
public class SBlockNodeMapping {

	public SBlockNodeMapping(String s, R2RMLModel mapConf) {
		this.mapconf = mapConf;
		this.s = s;
		init();
	}

	private void init() {

		this.ldps = mapconf.getLinkedDataPaths();
		for (String ldp : ldps) {
			mappings.addAll(mapconf.getMappings(ldp));
		}
		for (Mapping map : mappings) {
			var2Column.put(s, map.getIdColumn());
		}

	}

	private String s;
	private R2RMLModel mapconf;

	private Set<Mapping> mappings = new HashSet<Mapping>();
	private Set<String> ldps;

	// private Map<String, Set<Mapping>> ldp2mappings = new HashMap<String,
	// Set<Mapping>>();
	private Multimap<String, ColumDefinition> var2Column = HashMultimap
			.create();

	// public Map<String, Set<Mapping>> getLdp2mappings() {
	// return Collections.unmodifiableMap(ldp2mappings);
	// }

	protected Collection<ColumDefinition> getColumn(String var) {
		return Collections.unmodifiableCollection(var2Column.get(var));
	}
	
	public Collection<ColumDefinition> getColumn(String var, Mapping map) {
		Set<ColumDefinition> cols = new HashSet<ColumDefinition>();
		for(ColumDefinition col: getColumn(var)){
			if(col.getMapp().equals(map)){
				cols.add(col);
			}
		}
		
		return cols;
	}

	/**
	 * removes all ldps that are not mentioned here
	 * 
	 * @param ldps
	 */
	public boolean retainLdps(Collection<String> retainLdps) {

		// take care of the lds
		boolean hasretained = ldps.retainAll(retainLdps);

		if(hasretained){
		cleanUp();
		}
		return hasretained;

	}
	
	
	public String getS() {
		return s;
	}


	
	
	public boolean retainMappings(Collection<Mapping> retainMaps){
		
		boolean hasretained = mappings.retainAll(retainMaps);
		if(hasretained){
			cleanUp();
		}
		return hasretained;
	}
	
	public boolean retainColDefs(Multimap<String,ColumDefinition> retainColDefs){
		boolean hasretained = false;
		for(String var: retainColDefs.keySet()){
			hasretained = hasretained || var2Column.get(var).retainAll(retainColDefs.get(var));
		}
		if(hasretained){
			cleanUp();
		}
		return hasretained;
	}

	

	@Override
	public String toString() {
		return toString(0);
	}

	public String toString(int indent) {
		StringBuffer sb = new StringBuffer();
		String pre = "\n";
		for (int i = 0; i < indent; i++) {
			pre += "++";
		}

		sb.append(pre + "s-block, common s is:" + s.toString() + "");

		for (String var : var2Column.keySet()) {
			sb.append("\n" + pre + var.toString() + " maps to: ");
			for (ColumDefinition coldef : var2Column.get(var)) {
				sb.append(coldef.toString() + ", ");
			}
		}

		return sb.toString();
	}

	// public Set<ColumDefinition> getColdefsFor(Property prop) {
	// Set<ColumDefinition> cols = new HashSet<ColumDefinition>();
	//
	// for (String ldp : ldp2mappings.keySet()) {
	// for (Mapping map : ldp2mappings.get(ldp)) {
	// cols.addAll(map.getColumDefinition(prop));
	// }
	// }
	// return cols;
	// }

	public void addP(String objectVarName, String pUri) {

		// p is not defined, so we add all mappings.

		for (Mapping map : mappings) {
			if (pUri != null) {
				Collection<ColumDefinition> cds = map
						.getColumDefinition(pUri);
				if (cds != null && !cds.isEmpty()) {
					var2Column.putAll(objectVarName, cds);
				}

			} else {

				for (ColumDefinition coldef : map.getColDefinitions()) {
					if (!coldef.isIdColumn()) {
						var2Column.put(objectVarName, coldef);
					}
				}
			}
		}
	}

//	/**
//	 * removes all mappings and ldps that are not in use by some column
//	 */
//	public void cleanUpLdpsMappings() {
//		Set<String> toRetainLdp = null;
//		Set<Mapping> toRetainMap = null;
//		for (Node_Variable var : var2Column.keySet()) {
//			if (var != s) {
//				Set<String> ldpset = new HashSet<String>();
//				Set<Mapping> mapset = new HashSet<Mapping>();
//				Collection<ColumDefinition> coldefs = var2Column.get(var);
//				for (ColumDefinition coldef : coldefs) {
//					mapset.add(coldef.getMapp());
//					ldpset.add(coldef.getMapp().getLinkedDataPath());
//				}
//				
//				if(toRetainLdp == null){
//					toRetainLdp = ldpset;
//				}else{
//					toRetainLdp.retainAll(ldpset);
//				}
//				if(toRetainMap == null){
//					toRetainMap = mapset;
//				}else{
//					toRetainMap.retainAll(mapset);
//				}
//			}
//		}
//
//		//mappings.retainAll(toRetainMap);
//		if(!toRetainLdp.containsAll(ldps)){
//			retainLdps(toRetainLdp);
//		}
//		
//
//	}
	
	
	
	
	public void cleanUp(){
		// this should never run more than once, or?
		while (cleanUpLdps() | 	cleanUpMappings() |	cleanUpCols());
		

		


	
	
}

	private boolean cleanUpLdps() {
		//remove all ldps that no mapping pointing to
		Set<String> ldpsToRetain = new HashSet<String>();
		for(Mapping map : mappings){
			ldpsToRetain.add(map.getIdColumn().getLinkedDataPath());
		}
		boolean hasretained =  ldps.retainAll(ldpsToRetain);
		
		//remove all ldps that not colum is pointing to
		for(String var : var2Column.keySet()){
			Set<String> ldpsOfVar = new HashSet<String>();
			if(var != s){
				for(ColumDefinition coldef : var2Column.get(var)){
					ldpsOfVar.add(coldef.getMapp().getIdColumn().getLinkedDataPath());
				}
				
				hasretained = hasretained | ldps.retainAll(ldpsOfVar);
			}
		}
		
		
		
		return hasretained;
		
	}

	private boolean cleanUpMappings() {
		//remove all Mappings that do not have an ldp and are not used by a column

		Set<Mapping> mapsToRetain = new HashSet<Mapping>();
		for(Mapping map: mappings){
			if(ldps.contains(map.getIdColumn().getLinkedDataPath())){
				mapsToRetain.add(map);
			}
		}
		boolean hasretained = mappings.retainAll(mapsToRetain);
		mapsToRetain.clear();
		
		
		for(String var : var2Column.keySet()){
			if(var!=s){
				for(ColumDefinition coldef : var2Column.get(var)){
					mapsToRetain.add(coldef.getMapp());
				}
			}
		}

		hasretained = hasretained | mappings.retainAll(mapsToRetain);
			
		return 	hasretained;
	}

	private boolean cleanUpCols() {
		// remove all Columns that no have no ldps or mapping
		Set<ColumDefinition> colsToRetain = new HashSet<ColumDefinition>();
		
		
		for(ColumDefinition coldef : var2Column.values()){
			if(mappings.contains(coldef.getMapp()) && ldps.contains(coldef.getMapp().getIdColumn().getLinkedDataPath())){
				colsToRetain.add(coldef);
			}
		}
		
		return var2Column.values().retainAll(colsToRetain);
		
	}

	public Set<String> getLdps() {

		return ldps;
	}

	public Set<Mapping> getMappings() {

		return mappings;
	}

	public Set<Mapping> getMappings(String ldp) {
		Set<Mapping> mapps = new HashSet<Mapping>();
		for (Mapping map : mappings) {
			if (map.getIdColumn().getLinkedDataPath().equals(ldp)) {
				mapps.add(map);
			}
		}

		return mapps;
	}

}