# main.py - Enhanced with pipeline parsing and DAG validation

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
import json

app = FastAPI()

# Enable CORS for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Node(BaseModel):
    id: str
    type: str
    position: Dict[str, float]
    data: Dict[str, Any]

class Edge(BaseModel):
    id: str
    source: str
    target: str
    sourceHandle: str = None
    targetHandle: str = None

class PipelineRequest(BaseModel):
    nodes: List[Node]
    edges: List[Edge]

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

@app.post('/pipelines/parse')
async def parse_pipeline(pipeline: PipelineRequest):
    """
    Parse pipeline and return:
    - num_nodes: Number of nodes in the pipeline
    - num_edges: Number of edges in the pipeline  
    - is_dag: Whether the pipeline forms a Directed Acyclic Graph (DAG)
    """
    try:
        num_nodes = len(pipeline.nodes)
        num_edges = len(pipeline.edges)
        
        # Check if the pipeline forms a DAG (Directed Acyclic Graph)
        is_dag = check_dag(pipeline.nodes, pipeline.edges)
        
        return {
            'num_nodes': num_nodes,
            'num_edges': num_edges,
            'is_dag': is_dag
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing pipeline: {str(e)}")

def check_dag(nodes: List[Node], edges: List[Edge]) -> bool:
    """
    Check if the pipeline forms a Directed Acyclic Graph (DAG).
    Returns True if it's a valid DAG, False if there are cycles.
    """
    if not nodes or not edges:
        return True  # Empty or edge-less graphs are trivially DAGs
    
    # Build adjacency list representation
    graph = {}
    in_degree = {}
    
    # Initialize all nodes with empty adjacency list and in-degree 0
    for node in nodes:
        graph[node.id] = []
        in_degree[node.id] = 0
    
    # Build graph and calculate in-degrees
    for edge in edges:
        if edge.source in graph and edge.target in graph:
            graph[edge.source].append(edge.target)
            in_degree[edge.target] += 1
    
    # Kahn's algorithm for cycle detection
    # Start with nodes that have in-degree 0 (no incoming edges)
    queue = []
    for node_id in in_degree:
        if in_degree[node_id] == 0:
            queue.append(node_id)
    
    processed_count = 0
    
    while queue:
        # Remove a node with in-degree 0
        current = queue.pop(0)
        processed_count += 1
        
        # Reduce in-degree of all neighbors
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # If we processed all nodes, it's a DAG
    # If we processed fewer nodes, there are cycles
    return processed_count == len(nodes)

@app.get('/health')
def health_check():
    """Health check endpoint"""
    return {
        'status': 'healthy',
        'message': 'VectorShift Backend is running',
        'endpoints': {
            'root': '/',
            'parse_pipeline': '/pipelines/parse (POST)',
            'health': '/health'
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
