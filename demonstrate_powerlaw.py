import random
import networkx as nx
from collections import defaultdict

class NetworkBuilder:
    @staticmethod
    def powerlaw_subset(peers, existing_connections, k=2):
        """
        Select neighbors using preferential attachment (power-law distribution)
        Args:
            peers: List of (ip, port) tuples
            existing_connections: Current connections of the peer
            k: Minimum number of connections to maintain
        """
        if not peers:
            return []
            
        # Create frequency distribution
        degree_count = defaultdict(int)
        for ip, port in existing_connections:
            degree_count[(ip, port)] += 1
            
        # Convert to list with weights
        weighted_peers = []
        for peer in peers:
            weight = degree_count.get(peer, 1)  # Prefer nodes with higher degree
            weighted_peers.extend([peer]*weight)
            
        # Select at least k connections
        n = max(k, min(len(peers), 5))  # Connect to 5-15% of available peers
        try:
            selected = random.choices(
                weighted_peers, 
                k=random.randint(n, min(len(peers), n*3))
            )
            # Deduplicate and exclude self
            return list(set(selected))
        except:
            return random.sample(peers, min(len(peers), n))