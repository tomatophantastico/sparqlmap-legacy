package org.aksw.sparqlmap.config.syntax;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.aksw.sparqlmap.config.syntax.SimpleConfigParser;
import org.junit.Test;


public class SimpleConfigParserTest {

	@Test
	public void testParse() throws Throwable {
		
		
		SimpleConfigParser parser = new SimpleConfigParser();
		
		parser.parse(new InputStreamReader(ClassLoader.getSystemResourceAsStream("serendipity_test.r2r")));
		
		
		
	}

}
