import httplib
import math
import os
import os.path
import pickle
import random
import socket
import sys
import SimpleXMLRPCServer
import threading
import time
import xmlrpclib

node_ids = ["134.214.202.220", "134.214.202.221", "134.214.202.222"]


def get_election_timeout():
    # Timeout between 0.5 and 5 seconds
    return random.randint(500, 5000) / 1000.0


def get_heartbeat_timeout():
    return 5.0


class RaftNode(object):
    def __init__(self):

        self.id = ""
        self.nodes = []

        # persistent - update before responding to RPC
        self.currentTerm = 0
        self.votedFor = None
        self.numLog = 0
        self.log = []

        # volatile (all)
        self.commitIndex = 0
        self.lastApplied = 0

        # volatile (leader)
        self.nextIndex = []
        self.matchIndex = []

        self.state = 'Follower'

        self.electionTimeout = get_election_timeout()
        self.startTime = time.clock()
        self.votes = 0

        self.socketTimeout = None  # 0.05

        self.clientRunning = False
        self.leaderLock = threading.Lock()

        # Used for debugging
        self.nextState = 'None'
        self.nextTerm = -1

        # initial log entry
        entry = {"term": 1, "tid": -1, "data": "start"}

        self.numLog = 1
        self.log.append(entry)

        # load persistent data
        # if os.path.isfile('persistent'):
        #    self.readPersistent()
        # if os.path.isfile('raftlog'):
        #    self.readLog()

        self.running = True

    def terminate(self):
        self.running = False
        return 1

    def read_ip(self):
        os.system("hostname -i | cut -f1 -d' ' > address")
        with open('address', 'r') as f:
            self.id = f.read().strip()

    def write_log(self):
        with open('raftlog', 'w') as f:
            pickle.dump(self.log, f)

    def read_log(self):
        with open('raftlog', 'r') as f:
            self.log = pickle.load(f)
            self.numLog = len(self.log)

    def write_persistent(self):
        with open('persistent', 'w') as f:
            pickle.dump([self.currentTerm, self.votedFor], f)

    def read_persistent(self):
        with open('persistent', 'r') as f:
            line = pickle.load(f)
            self.currentTerm = line[0]
            self.votedFor = line[1]

    def set_current_term(self, t):
        self.currentTerm = t
        self.write_persistent()

    def set_voted_for(self, v):
        self.votedFor = v
        self.write_persistent()

    def get_log_index(self):
        return self.numLog - 1

    def get_log_term(self):
        if self.numLog == 0:
            return 0
        entry = self.log[self.numLog - 1]
        return entry["term"]

    def init_follower(self, term):
        # print 'Convert to Follower'
        self.state = 'Follower'
        self.set_current_term(term)
        self.set_voted_for(None)

    def init_candidate(self):
        # print 'Convert to Candidate'
        self.state = 'Candidate'
        self.set_current_term(self.currentTerm + 1)
        self.electionTimeout = get_election_timeout()
        # print self.electionTimeout
        self.startTime = time.clock()

    def init_leader(self):
        # print 'Convert to Leader'
        self.state = 'Leader'
        self.nextIndex = []
        self.matchIndex = []

        for _ in self.nodes:
            self.nextIndex.append(self.get_log_index() + 1)
            self.matchIndex.append(0)

        # Send empty appendEntriesRPC() entries to followers
        self.update_leader()

    # Check if election timeout elapses. If so, convert to candidate.
    def update_follower(self):
        if (self.startTime + self.electionTimeout) < time.clock():
            self.init_candidate()

    # Send vote requests to all other nodes
    def update_candidate(self):
        self.votes = 1
        self.set_voted_for(self.id)
        for i in range(0, len(self.nodes)):

            n = self.nodes[i]
            last_log_index = self.get_log_index()
            last_log_term = self.get_log_term()

            try:
                socket.setdefaulttimeout(self.socketTimeout)
                t, v = n.request_vote_rpc(self.currentTerm, self.id, last_log_index, last_log_term)
                socket.setdefaulttimeout(None)

                if t > self.currentTerm:
                    self.init_follower(t)
                    break
                if v:
                    self.votes += 1

            except httplib.HTTPException, e:
                print str(e) + " on node " + node_ids[i]
            except Exception, e:
                print str(e) + " on node " + node_ids[i]

        if self.votes >= (len(self.nodes)+1)/ 2.0:
            self.init_leader()
        else:
            self.init_follower(self.currentTerm)
            self.electionTimeout = get_election_timeout()
            self.startTime = time.clock()

    # Make sure log entries on other servers are up to date
    def update_leader(self):

        self.leaderLock.acquire()

        commit_count = 1
        for i in range(0, len(self.nodes)):

            n = self.nodes[i]
            prev_log_index = self.nextIndex[i] - 1
            # print self.nextIndex[i]
            # print self.log
            prev_log_term = self.log[prev_log_index]["term"]
            entries = self.log[self.nextIndex[i]:]

            try:
                socket.setdefaulttimeout(self.socketTimeout)
                t, v = n.append_entries_rpc(self.currentTerm, self.id,
                                            prev_log_index, prev_log_term,
                                            entries, self.commitIndex)
                socket.setdefaulttimeout(None)

                if t > self.currentTerm:
                    self.init_follower(t)
                    break
                if v:
                    self.nextIndex[i] = self.get_log_index() + 1
                    commit_count += 1
                else:
                    self.nextIndex[i] -= 1

            except httplib.HTTPException, e:
                print str(e) + " on node " + node_ids[i]
            except Exception, e:
                print str(e) + " on node " + node_ids[i]

        self.leaderLock.release()

        return commit_count

    def run(self):
        # print self.log
        if (self.nextState != self.state) or (self.nextTerm != self.currentTerm):
            print 'Run ' + self.state + ' ' + str(self.currentTerm)
            self.nextState = self.state
            self.nextTerm = self.currentTerm

        if self.commitIndex > self.lastApplied:
            self.lastApplied += 1
            # apply self.log[self.lastApplied] to state machine

        if self.state == 'Follower':
            self.update_follower()

        elif self.state == 'Candidate':
            self.update_candidate()

        elif self.state == 'Leader':
            if not self.clientRunning:
                self.update_leader()

    # Invoked by candidates to gather votes
    def request_vote_rpc(self, term, candidate_id, last_log_index, last_log_term):
        print 'Vote Request ' + candidate_id + ' ' + str(self.currentTerm) + ' ' + str(term)
        if term < self.currentTerm:
            return self.currentTerm, False
        elif term > self.currentTerm:
            self.init_follower(term)

        if (self.votedFor is None) or (self.votedFor == candidate_id):
            # Return False is voter has more complete log
            if (self.get_log_term() > last_log_term) or \
                            (self.get_log_term() == last_log_term) and \
                            (self.get_log_index() > last_log_index):
                return self.currentTerm, False
            else:
                self.set_voted_for(candidate_id)
                return self.currentTerm, True

        return self.currentTerm, False

    # Invoked by leader to replicate log entries
    # Also used as a heartbeat
    def append_entries_rpc(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        print 'Heartbeat ' + leader_id + ' ' + str(self.currentTerm) + ' ' + str(term) + ' ' + str(entries)
        if term < self.currentTerm:
            return self.currentTerm, False
        elif term > self.currentTerm:
            self.init_follower(term)

        # Reset timeout
        self.electionTimeout = get_heartbeat_timeout()
        self.startTime = time.clock()

        if prev_log_index < len(self.log):
            entry = self.log[prev_log_index]
            log_term = entry["term"]
            # print 'log_term ' + str(log_term)

            if log_term == prev_log_term:
                # print self.log
                self.log = self.log[:(prev_log_index + 1)] + entries
                # print self.log
                self.write_log()

                if self.commitIndex < leader_commit:
                    self.commitIndex = leader_commit

                # print 'append True'
                return self.currentTerm, True

        return self.currentTerm, False

    # Invoked by clients
    # Returns true if this node is the leader
    def is_leader(self):
        return self.state == 'Leader'

    # Invoked by clients to add entry to state machine
    def add_entry(self, tid, data):

        print 'addEntry ' + str(tid) + ' ' + data

        self.clientRunning = True

        entry = {"term": self.currentTerm, "tid": tid, "data": data}

        # Note: transaction id should not already be in log
        #       to avoid duplicate transactions

        self.numLog += 1
        self.log.append(entry)
        self.write_log()

        committed = False

        count = 0

        while not committed and self.state == 'Leader':
            # print committed
            commit_count = self.update_leader()

            # Commit entries
            if commit_count > math.ceil((len(self.nodes) + 1) / 2.0):
                committed = True
                self.commitIndex += 1

            count += 1
            if count == 20:
                break

        self.clientRunning = False

        return committed


def main(argv):
    print argv
    node = RaftNode()

    node.read_ip()
    print "Id: " + node.id
    print "Timeout: " + str(node.electionTimeout)
    node_ids.remove(node.id)
    print node_ids

    # print node.id
    # print node_ids

    server = SimpleXMLRPCServer.SimpleXMLRPCServer(("", 8000), allow_none=True)
    server.register_instance(node)

    for nodeId in node_ids:
        n = xmlrpclib.Server("http://" + nodeId + ":8000", allow_none=True)
        node.nodes.append(n)

    print("Raft node ready.")
    # server.serve_forever()

    t = threading.Thread(target=server.serve_forever)
    t.daemon=True
    t.start()

    while node.running:
        node.run()


if __name__ == "__main__":
    main(sys.argv[1:])
