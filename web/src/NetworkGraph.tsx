import { useEffect, useRef } from "react";
import {
  drag,
  forceCenter,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
  select,
  zoom,
  type SimulationLinkDatum,
  type SimulationNodeDatum,
} from "d3";
import { riskBand } from "./graph";
import type { DashboardEdge, GraphViewNode } from "./types";

interface GraphNode extends GraphViewNode, SimulationNodeDatum {}

interface GraphLink extends SimulationLinkDatum<GraphNode> {
  id: string;
  source: string | GraphNode;
  target: string | GraphNode;
  amount: number;
  risk_indicators: string[];
}

interface NetworkGraphProps {
  nodes: GraphViewNode[];
  edges: DashboardEdge[];
  selectedNodeId: string | null;
  selectedEdgeId: string | null;
  onSelectNode: (nodeId: string) => void;
  onSelectEdge: (edgeId: string) => void;
  onExpandCommunity: (communityId: number) => void;
}

const WIDTH = 1000;
const HEIGHT = 620;

function nodeColour(node: GraphViewNode): string {
  if (node.kind === "community") return "#7aa6ff";
  const band = riskBand(node.graph_risk_score);
  if (band === "critical") return "#ff665f";
  if (band === "watch") return "#e4b15b";
  return "#42d6a4";
}

function linkWidth(link: GraphLink): number {
  return Math.max(1, Math.min(4, Math.log10(link.amount + 1) - 1));
}

function linkEndpointId(value: string | GraphNode): string {
  return typeof value === "object" ? value.id : value;
}

function endpoint(
  value: string | number | GraphNode,
  nodes: Map<string, GraphNode>,
): GraphNode {
  if (typeof value === "object") return value;
  const node = nodes.get(String(value));
  if (!node) throw new Error(`Missing graph endpoint: ${String(value)}`);
  return node;
}

export default function NetworkGraph({
  nodes,
  edges,
  selectedNodeId,
  selectedEdgeId,
  onSelectNode,
  onSelectEdge,
  onExpandCommunity,
}: NetworkGraphProps) {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current || nodes.length === 0) return undefined;

    const svg = select(svgRef.current);
    svg.selectAll("*").remove();
    svg.attr("viewBox", `0 0 ${WIDTH} ${HEIGHT}`);

    const graphNodes: GraphNode[] = nodes.map((node, index) => {
      const angle = (index / Math.max(nodes.length, 1)) * Math.PI * 2;
      return {
        ...node,
        x: WIDTH / 2 + Math.cos(angle) * Math.min(240, nodes.length * 8),
        y: HEIGHT / 2 + Math.sin(angle) * Math.min(210, nodes.length * 8),
      };
    });
    const nodeById = new Map(graphNodes.map((node) => [node.id, node]));
    const graphLinks: GraphLink[] = edges
      .filter((edge) => nodeById.has(edge.source) && nodeById.has(edge.target))
      .map((edge) => ({
        ...edge,
        source: edge.source,
        target: edge.target,
      }));
    const activateNode = (node: GraphNode) => {
      if (node.kind === "community" && node.community_id !== null) {
        onExpandCommunity(node.community_id);
        return;
      }
      onSelectNode(node.id);
    };

    const definitions = svg.append("defs");
    definitions
      .append("marker")
      .attr("id", "transfer-arrow")
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 22)
      .attr("refY", 0)
      .attr("markerWidth", 5)
      .attr("markerHeight", 5)
      .attr("orient", "auto")
      .append("path")
      .attr("d", "M0,-5L10,0L0,5")
      .attr("fill", "#769089");

    const canvas = svg.append("g");
    svg.call(
      zoom<SVGSVGElement, unknown>()
        .scaleExtent([0.45, 4])
        .on("zoom", (event) => canvas.attr("transform", event.transform)),
    );

    const links = canvas
      .append("g")
      .attr("class", "graph-links")
      .selectAll<SVGLineElement, GraphLink>("line")
      .data(graphLinks, (link) => link.id)
      .join("line")
      .attr("stroke", (link) =>
        link.risk_indicators.length > 0 ? "#ae655f" : "#37564f",
      )
      .attr("stroke-width", linkWidth)
      .attr("stroke-opacity", 0.72)
      .attr("marker-end", "url(#transfer-arrow)");

    links
      .append("title")
      .text(
        (link) =>
          `${link.id}\n$${link.amount.toLocaleString()}\n${link.risk_indicators.join(", ") || "No indicators"}`,
      );

    const linkHitboxes = canvas
      .append("g")
      .attr("class", "graph-link-hitboxes")
      .selectAll<SVGLineElement, GraphLink>("line")
      .data(graphLinks, (link) => link.id)
      .join("line")
      .attr("class", "graph-link-hit")
      .attr("stroke", "transparent")
      .attr("stroke-width", 16)
      .attr("tabindex", 0)
      .attr("role", "button")
      .attr(
        "aria-label",
        (link) =>
          `Inspect transfer ${link.id} from ${linkEndpointId(link.source)} to ${linkEndpointId(link.target)}, amount ${link.amount}`,
      )
      .on("click", (_, link) => onSelectEdge(link.id))
      .on("keydown", (event, link) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onSelectEdge(link.id);
        }
      });

    const nodeGroups = canvas
      .append("g")
      .attr("class", "graph-nodes")
      .selectAll<SVGGElement, GraphNode>("g")
      .data(graphNodes, (node) => node.id)
      .join("g")
      .attr("class", (node) =>
        node.kind === "community" ? "graph-node community-node" : "graph-node",
      )
      .attr("tabindex", 0)
      .attr("role", "button")
      .attr("aria-label", (node) =>
        node.kind === "community"
          ? `Expand community ${node.community_id}, ${node.member_count} accounts, maximum risk score ${node.graph_risk_score}`
          : `Inspect ${node.label}, risk score ${node.graph_risk_score}`,
      )
      .on("click", (_, node) => activateNode(node))
      .on("keydown", (event, node) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          activateNode(node);
        }
      });

    nodeGroups
      .append("circle")
      .attr("class", "node-circle")
      .attr("r", (node) =>
        node.kind === "community"
          ? 18 + Math.min(14, Math.sqrt(node.member_count) * 3)
          : 8 + Math.min(9, node.graph_risk_score / 12),
      )
      .attr("fill", nodeColour)
      .attr("stroke", "#071411")
      .attr("stroke-width", 3);

    nodeGroups
      .append("text")
      .attr("class", "node-label")
      .attr("x", 18)
      .attr("y", 4)
      .text((node) => node.label)
      .style("display", (node) =>
        node.kind === "community" || node.graph_risk_score >= 70 || nodes.length <= 20
          ? "block"
          : "none",
      );

    nodeGroups
      .append("title")
      .text(
        (node) =>
          node.kind === "community"
            ? `${node.label}\n${node.member_count} accounts · max risk ${node.graph_risk_score.toFixed(1)}\nSelect to expand`
            : `${node.label}\nRisk ${node.graph_risk_score.toFixed(1)} · PageRank ${node.pagerank_score.toFixed(3)}`,
      );

    const simulation = forceSimulation(graphNodes)
      .force(
        "link",
        forceLink<GraphNode, GraphLink>(graphLinks)
          .id((node) => node.id)
          .distance((link) => 80 + Math.min(80, 15000 / (link.amount + 250)))
          .strength(0.32),
      )
      .force("charge", forceManyBody().strength(-210))
      .force("collision", forceCollide<GraphNode>().radius((node) => 30 + node.graph_risk_score / 6))
      .force("center", forceCenter(WIDTH / 2, HEIGHT / 2))
      .alphaDecay(0.035);

    nodeGroups.call(
      drag<SVGGElement, GraphNode>()
        .on("start", (event, node) => {
          if (!event.active) simulation.alphaTarget(0.25).restart();
          node.fx = node.x;
          node.fy = node.y;
        })
        .on("drag", (event, node) => {
          node.fx = event.x;
          node.fy = event.y;
        })
        .on("end", (event, node) => {
          if (!event.active) simulation.alphaTarget(0);
          node.fx = null;
          node.fy = null;
        }),
    );

    simulation.on("tick", () => {
      links
        .attr("x1", (link) => endpoint(link.source, nodeById).x ?? 0)
        .attr("y1", (link) => endpoint(link.source, nodeById).y ?? 0)
        .attr("x2", (link) => endpoint(link.target, nodeById).x ?? 0)
        .attr("y2", (link) => endpoint(link.target, nodeById).y ?? 0);
      linkHitboxes
        .attr("x1", (link) => endpoint(link.source, nodeById).x ?? 0)
        .attr("y1", (link) => endpoint(link.source, nodeById).y ?? 0)
        .attr("x2", (link) => endpoint(link.target, nodeById).x ?? 0)
        .attr("y2", (link) => endpoint(link.target, nodeById).y ?? 0);
      nodeGroups.attr(
        "transform",
        (node) => `translate(${node.x ?? 0},${node.y ?? 0})`,
      );
    });

    return () => {
      simulation.stop();
    };
  }, [edges, nodes, onExpandCommunity, onSelectEdge, onSelectNode]);

  useEffect(() => {
    if (!svgRef.current) return;
    select(svgRef.current)
      .selectAll<SVGCircleElement, GraphNode>(".node-circle")
      .attr("stroke", (node) =>
        node.id === selectedNodeId ? "#f3fffb" : "#071411",
      )
      .attr("stroke-width", (node) => (node.id === selectedNodeId ? 5 : 3));
  }, [selectedNodeId]);

  useEffect(() => {
    if (!svgRef.current) return;
    select(svgRef.current)
      .selectAll<SVGLineElement, GraphLink>(".graph-links line")
      .attr("stroke", (link) =>
        link.id === selectedEdgeId
          ? "#f3fffb"
          : link.risk_indicators.length > 0
            ? "#ae655f"
            : "#37564f",
      )
      .attr("stroke-opacity", (link) =>
        link.id === selectedEdgeId ? 1 : 0.72,
      )
      .attr("stroke-width", (link) =>
        linkWidth(link) + (link.id === selectedEdgeId ? 2 : 0),
      );
  }, [selectedEdgeId]);

  return (
    <svg
      ref={svgRef}
      className="network-graph"
      role="img"
      aria-label="Directed network of account transfers. Drag nodes or use the mouse wheel to explore."
    />
  );
}
