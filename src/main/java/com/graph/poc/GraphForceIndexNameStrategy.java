package com.graph.poc;

import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;

public class GraphForceIndexNameStrategy {
	private static final Logger logger=Logger.getLogger(GraphForceIndexNameStrategy.class);

	private Graph graph;

	public GraphForceIndexNameStrategy(Graph g) {
		this.graph = g;
	}

	public void forceIndexConfiguration() {
		JanusGraph jGraph = (JanusGraph) graph;
		JanusGraphManagement jMgmt = jGraph.openManagement();
		Set<String> open = jMgmt.getOpenInstances();
		logger.warn("Open graph instances - "+open);
		for (String string : open) {

			if (!string.endsWith("(current)")) {
				logger.warn("Closing - " + string);
				jMgmt.forceCloseInstance(string);
			} else {
				logger.warn("Not closing-" + string);
			}

		}
		logger.warn("Open graph instances , after closing before commit- "+jMgmt.getOpenInstances());
		jMgmt.commit();
		logger.warn("Open graph instances , after closing before commit- "+jMgmt.getOpenInstances());
		jMgmt = jGraph.openManagement();
		jMgmt.set("index.search.index-name", "titan");
		jMgmt.commit();
		logger.warn("Graph status -isClosed - "+jGraph.isClosed());
	}

}
