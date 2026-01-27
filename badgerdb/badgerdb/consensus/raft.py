"""
Raft Consensus Implementation

Provides leader election and log replication for HA sequencers.
Based on the Raft paper (Ongaro & Ousterhout, 2014).
"""

from __future__ import annotations
import threading
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
from enum import Enum
from queue import Queue
import logging

logger = logging.getLogger(__name__)


class RaftState(Enum):
    """Raft node states."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """A single entry in the Raft log."""
    term: int
    index: int
    command: Any

    def __hash__(self):
        return hash((self.term, self.index))


@dataclass
class AppendEntriesRequest:
    """AppendEntries RPC request."""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    """AppendEntries RPC response."""
    term: int
    success: bool
    match_index: int = 0


@dataclass
class RequestVoteRequest:
    """RequestVote RPC request."""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    """RequestVote RPC response."""
    term: int
    vote_granted: bool


class RaftNode:
    """
    A single Raft node.

    Handles leader election and log replication.
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        apply_callback: Callable[[Any], Any],
        election_timeout_range: tuple = (150, 300),  # ms
        heartbeat_interval: int = 50  # ms
    ):
        self.node_id = node_id
        self.peers = peers
        self.apply_callback = apply_callback
        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval / 1000.0

        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []

        # Volatile state
        self.state = RaftState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0

        # Leader state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Timing
        self._last_heartbeat = time.time()
        self._election_timeout = self._random_election_timeout()

        # Threading
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Message queues (simulated network)
        self._inbox: Queue = Queue()
        self._cluster: Optional[RaftCluster] = None

        # Pending client requests
        self._pending: Dict[int, threading.Event] = {}
        self._results: Dict[int, Any] = {}

    def _random_election_timeout(self) -> float:
        """Get random election timeout in seconds."""
        lo, hi = self.election_timeout_range
        return random.randint(lo, hi) / 1000.0

    def start(self):
        """Start the Raft node."""
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the Raft node."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)

    def _run_loop(self):
        """Main event loop."""
        while self._running:
            self._process_messages()
            self._check_timeouts()
            self._apply_committed()
            time.sleep(0.01)  # 10ms tick

    def _process_messages(self):
        """Process incoming messages."""
        while not self._inbox.empty():
            try:
                msg = self._inbox.get_nowait()
                self._handle_message(msg)
            except:
                break

    def _handle_message(self, msg):
        """Handle an incoming message."""
        if isinstance(msg, RequestVoteRequest):
            response = self._handle_request_vote(msg)
            self._send(msg.candidate_id, response)
        elif isinstance(msg, RequestVoteResponse):
            self._handle_request_vote_response(msg)
        elif isinstance(msg, AppendEntriesRequest):
            response = self._handle_append_entries(msg)
            self._send(msg.leader_id, response)
        elif isinstance(msg, AppendEntriesResponse):
            self._handle_append_entries_response(msg)

    def _handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        """Handle RequestVote RPC."""
        with self._lock:
            if req.term > self.current_term:
                self._become_follower(req.term)

            vote_granted = False
            if req.term >= self.current_term:
                if self.voted_for is None or self.voted_for == req.candidate_id:
                    # Check if candidate's log is at least as up-to-date
                    last_log_term = self.log[-1].term if self.log else 0
                    last_log_index = len(self.log)

                    if (req.last_log_term > last_log_term or
                        (req.last_log_term == last_log_term and
                         req.last_log_index >= last_log_index)):
                        vote_granted = True
                        self.voted_for = req.candidate_id
                        self._last_heartbeat = time.time()

            return RequestVoteResponse(
                term=self.current_term,
                vote_granted=vote_granted
            )

    def _handle_request_vote_response(self, resp: RequestVoteResponse):
        """Handle RequestVote response."""
        with self._lock:
            if resp.term > self.current_term:
                self._become_follower(resp.term)
                return

            if self.state != RaftState.CANDIDATE:
                return

            if resp.vote_granted:
                self._votes_received.add(resp.term)  # Track by term for simplicity

                # Check if we have majority
                if len(self._votes_received) >= (len(self.peers) + 1) // 2 + 1:
                    self._become_leader()

    def _handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handle AppendEntries RPC."""
        with self._lock:
            if req.term > self.current_term:
                self._become_follower(req.term)
            elif req.term < self.current_term:
                return AppendEntriesResponse(term=self.current_term, success=False)

            # Valid leader
            self._last_heartbeat = time.time()
            self.state = RaftState.FOLLOWER

            # Check log consistency
            if req.prev_log_index > 0:
                if len(self.log) < req.prev_log_index:
                    return AppendEntriesResponse(term=self.current_term, success=False)
                if self.log[req.prev_log_index - 1].term != req.prev_log_term:
                    # Truncate inconsistent entries
                    self.log = self.log[:req.prev_log_index - 1]
                    return AppendEntriesResponse(term=self.current_term, success=False)

            # Append new entries
            for entry in req.entries:
                if entry.index <= len(self.log):
                    if self.log[entry.index - 1].term != entry.term:
                        self.log = self.log[:entry.index - 1]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            # Update commit index
            if req.leader_commit > self.commit_index:
                self.commit_index = min(req.leader_commit, len(self.log))

            return AppendEntriesResponse(
                term=self.current_term,
                success=True,
                match_index=len(self.log)
            )

    def _handle_append_entries_response(self, resp: AppendEntriesResponse):
        """Handle AppendEntries response (leader only)."""
        with self._lock:
            if resp.term > self.current_term:
                self._become_follower(resp.term)
                return

            if self.state != RaftState.LEADER:
                return

            # Update match_index and next_index (simplified)
            # In real implementation, track per-peer

    def _check_timeouts(self):
        """Check for election timeout."""
        with self._lock:
            now = time.time()

            if self.state == RaftState.LEADER:
                # Send heartbeats
                if now - self._last_heartbeat >= self.heartbeat_interval:
                    self._send_heartbeats()
                    self._last_heartbeat = now
            else:
                # Check election timeout
                if now - self._last_heartbeat >= self._election_timeout:
                    self._start_election()

    def _start_election(self):
        """Start a new election."""
        with self._lock:
            self.current_term += 1
            self.state = RaftState.CANDIDATE
            self.voted_for = self.node_id
            self._votes_received = {self.node_id}
            self._last_heartbeat = time.time()
            self._election_timeout = self._random_election_timeout()

            # Request votes from all peers
            last_log_term = self.log[-1].term if self.log else 0
            last_log_index = len(self.log)

            for peer in self.peers:
                self._send(peer, RequestVoteRequest(
                    term=self.current_term,
                    candidate_id=self.node_id,
                    last_log_index=last_log_index,
                    last_log_term=last_log_term
                ))

    def _become_follower(self, term: int):
        """Transition to follower state."""
        self.state = RaftState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self._election_timeout = self._random_election_timeout()

    def _become_leader(self):
        """Transition to leader state."""
        self.state = RaftState.LEADER

        # Initialize leader state
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0

        # Send initial heartbeats
        self._send_heartbeats()

    def _send_heartbeats(self):
        """Send heartbeat to all followers."""
        for peer in self.peers:
            prev_log_index = self.next_index.get(peer, 1) - 1
            prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 and self.log else 0

            # Get entries to send
            entries = self.log[prev_log_index:] if prev_log_index < len(self.log) else []

            self._send(peer, AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index
            ))

    def _apply_committed(self):
        """Apply committed entries to state machine."""
        with self._lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]

                # Apply to state machine
                result = self.apply_callback(entry.command)

                # Notify waiting clients
                if entry.index in self._pending:
                    self._results[entry.index] = result
                    self._pending[entry.index].set()

    def _send(self, to: str, msg):
        """Send message to another node."""
        if self._cluster:
            self._cluster.route_message(self.node_id, to, msg)

    def propose(self, command: Any, timeout: float = 5.0) -> Any:
        """
        Propose a new command (leader only).

        Returns result after command is committed and applied.
        """
        with self._lock:
            if self.state != RaftState.LEADER:
                raise Exception("Not leader")

            # Append to log
            index = len(self.log) + 1
            entry = LogEntry(
                term=self.current_term,
                index=index,
                command=command
            )
            self.log.append(entry)

            # Create event for waiting
            event = threading.Event()
            self._pending[index] = event

        # Wait for commit
        if event.wait(timeout):
            with self._lock:
                return self._results.pop(index, None)
        else:
            raise Exception("Timeout waiting for commit")

    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        return self.state == RaftState.LEADER

    def get_leader(self) -> Optional[str]:
        """Get current leader (if known)."""
        if self.state == RaftState.LEADER:
            return self.node_id
        return None  # Simplified - would track leader in real impl


class RaftCluster:
    """
    A cluster of Raft nodes.

    Handles message routing between nodes.
    """

    def __init__(self):
        self.nodes: Dict[str, RaftNode] = {}
        self._lock = threading.Lock()

    def add_node(self, node: RaftNode):
        """Add a node to the cluster."""
        with self._lock:
            self.nodes[node.node_id] = node
            node._cluster = self

    def remove_node(self, node_id: str):
        """Remove a node from the cluster."""
        with self._lock:
            if node_id in self.nodes:
                self.nodes[node_id]._cluster = None
                del self.nodes[node_id]

    def route_message(self, from_id: str, to_id: str, msg):
        """Route a message between nodes."""
        with self._lock:
            if to_id in self.nodes:
                self.nodes[to_id]._inbox.put(msg)

    def start_all(self):
        """Start all nodes."""
        for node in self.nodes.values():
            node.start()

    def stop_all(self):
        """Stop all nodes."""
        for node in self.nodes.values():
            node.stop()

    def get_leader(self) -> Optional[RaftNode]:
        """Get the current leader node."""
        for node in self.nodes.values():
            if node.is_leader():
                return node
        return None

    def wait_for_leader(self, timeout: float = 5.0) -> Optional[RaftNode]:
        """Wait for a leader to be elected."""
        start = time.time()
        while time.time() - start < timeout:
            leader = self.get_leader()
            if leader:
                return leader
            time.sleep(0.05)
        return None
