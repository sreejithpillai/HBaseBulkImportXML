package com.sreejithpillai.hbase.bulkload;

import java.io.ByteArrayInputStream;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.StreamFilter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class Utils {

	public String[] getXmlTags(String line, List<String> columnList) {
		
		String[] fields = new String[7];
		
		try {
						
			XMLInputFactory factory = XMLInputFactory.newFactory();
			
			XMLStreamReader rawReader = factory.createXMLStreamReader(new ByteArrayInputStream(line.getBytes()));
			
			
			XMLStreamReader filteredReader = factory.createFilteredReader(rawReader,
					  new StreamFilter() {
					    public boolean accept(XMLStreamReader r) {
					      return !r.isWhiteSpace();
					    }
					  });
			String currentElement = "";

			while (filteredReader.hasNext()) {
				System.out.println("reader");
				int code = filteredReader.next();
				
				switch (code) {
				
				case XMLStreamReader.START_ELEMENT:
					currentElement = filteredReader.getLocalName();
					System.out.println("current element "+ currentElement);
					break;
					
				
				case XMLStreamReader.CHARACTERS:
					
					int k = 0;
					for (String xmlTag : columnList) {
						
						if (currentElement.equalsIgnoreCase(xmlTag)) {
							fields[k] = filteredReader.getText().trim();
						}
						k++;
					}

				}
			}
		} catch (XMLStreamException e) {
			e.printStackTrace();
		} catch (FactoryConfigurationError e) {
			e.printStackTrace();
		}
		return fields;	
	}

}

