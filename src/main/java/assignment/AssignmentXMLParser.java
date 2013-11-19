package assignment;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class AssignmentXMLParser {

	private static void addAssignment(Integer threadId, SortedSet<String> payloadSet,
			HashMap<Integer, SortedSet<String> > assignMap) {
		if (!assignMap.containsKey(threadId)) {
			// Create as new.
			assignMap.put(threadId, new TreeSet<String>(payloadSet));
		} else {
			// Accumulate.
			assignMap.get(threadId).addAll(payloadSet);
		}
	}

	public static HashMap<Integer, SortedSet<String> > parse(String assignmentFilePath) {
		HashMap<Integer, SortedSet<String> > result = new HashMap<Integer, SortedSet<String>>();
		try {
			File asFile = new File(assignmentFilePath);

			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(asFile);
			doc.getDocumentElement().normalize();

			NodeList nList = doc.getElementsByTagName("payload");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					SortedSet<String> plSet = new TreeSet<String>();
					plSet.add(eElement.getAttribute("name"));
					NodeList nl = eElement.getElementsByTagName("threadIDList");
					if (nl.getLength() != 0) {
						for (int i = 0; i < nl.getLength(); ++i) {
							String[] idsStr = nl.item(i).getTextContent().trim().split("\\s+");
							for (int j = 0; j < idsStr.length; ++j) {
								addAssignment(Integer.valueOf(idsStr[j]), plSet, result);
							}
						}
					}
					nl = eElement.getElementsByTagName("threadIDRange");
					if (nl.getLength() != 0) {
						for (int i = 0; i < nl.getLength(); ++i) {
							Element idR = (Element) nl.item(i);
							int from = Integer.valueOf(idR.getAttribute("from"));
							int to = Integer.valueOf(idR.getAttribute("to"));
							for (int j = from; j <= to; ++j) addAssignment(j, plSet, result);
						}
					}
				}
			}

			nList = doc.getElementsByTagName("threads");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Element nEl = (Element) nList.item(temp);
				SortedSet<String> pls = new TreeSet<String>();
				NodeList plsNL = nEl.getElementsByTagName("payloadList");
				for (int i = 0; i < plsNL.getLength(); ++i) {
					pls.addAll(Arrays.asList(plsNL.item(i).getTextContent().trim().split("\\s+")));
				}
				Node idListAtt = nEl.getAttributeNode("idList");
				Node idRangeAtt = nEl.getAttributeNode("idRange");
				if (idListAtt != null) {
					String[] idsStr = idListAtt.getTextContent().trim().split("\\s+");
					for (int i = 0; i < idsStr.length; ++i) {
						addAssignment(Integer.valueOf(idsStr[i]), pls, result);
					}
				}
				if (idRangeAtt != null) {
					String[] range = idRangeAtt.getTextContent().split("-");
					int from = Integer.valueOf(range[0]);
					int to = Integer.valueOf(range[1]);
					for (int i = from; i <= to; ++i) {
						addAssignment(i, pls, result);
					}
				}
				if (idListAtt == null && idRangeAtt == null) {
					System.out.println(" -> Neither idList or idRange was found! Is this intentional?");
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.toString());
		}
		return result;
	}

}
