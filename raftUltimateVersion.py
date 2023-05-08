#!/usr/bin/env python

import logging
from ms import receiveAll, reply, send
import random
import threading

        
logging.getLogger().setLevel(logging.DEBUG)

class Raft:
    def __init__(self):
        ## General variables
        # Node and network identity
        self.node_id=None
        self.neighbours = []
        self.majority = 0        # Majority of nodes

        # State
        self.current_term = 0       # Termo atual
        self.voted_for = None       # Variable to be reset each new term
        self.logs = []              # Change log # Each log will be composed of a tuple: (key,value,requesterMSG,term)
        self.kv = {}                # key value dictionary
        self.commitIndex = -1       # Index of the last commited log
        self.lastAppended = -1      # Index of the last appended log
        self.tHeartbeat = None      # Timer for a heartbit
        self.tElection = None       # Timer for election
        self.heartBeatTimer = 0.050 # Timeout for heartbeat
        self.electionTimer = random.randint(150,300) * 0.0005 # Timeout for election
        self.leader = None          # Last src of an AppendEntrie
        self.q_buffer = {}          # Quorum messages that need to be treated
  
        ## Leader Variables
        self.nextIndex = {}      # Map node to the list of log indexes that need to be sent to the node
        self.matchIndex= {}      # Index do maior log que os nodos devem ter (segundo o lider)
        self.messages = {}       # Map to store the messages of the client, so it can then respond
        self.candidateCounter=0  # Counter for the number of positive receptions
        self.votesReceived=0     # Counter for the number of votes received (positive or negative)

    def imLeader(self):
        return self.voted_for==self.node_id and self.tHeartbeat!=None

    def electionTimeOut(self):
        logging.info("Election Timeout! Start Candidate process on node: %s" + str(self.voted_for))

        ##Code
        # Voted for self
        self.voted_for = self.node_id

        # Current term increase
        self.current_term+=1

        # Voted for self, so counter on 1
        self.candidateCounter=1
        self.votesReceived=1

        lastLogterm =0
        if len(self.logs)>0:
            lastLogterm = self.logs[self.lastAppended][3]

        #Send out vote requests
        for n in self.neighbours:
            send(self.node_id,n,
                type="vote_request",
                term= self.current_term,
                commitedLogs= self.commitIndex,
                lastLogIndex= self.lastAppended,
                lastLogTerm = lastLogterm)

        # Restart tElectiontimer
        self.electionTimer = random.randint(150,300) * 0.001
        self.tElection = threading.Timer(self.electionTimer , self.electionTimeOut)
        self.tElection.start()
        
    def heartbeatTimeOut(self):
        #Logs
        logging.info("Send out heartbeat")

        #Code
        for n in self.neighbours:
            entries,prevLog,prevLogTerm=self.selectEntries(n)

            send(self.node_id,
                    n,
                    type        = "append_entries",
                    term        = self.current_term,
                    commitedLogs= self.commitIndex,
                    leaderId    = self.node_id,
                    entries     = entries,
                    prevLog     = prevLog,
                    prevLogTerm = prevLogTerm,
                    appendIndex = self.lastAppended)

        if self.imLeader():
            self.tHeartbeat = threading.Timer(self.heartBeatTimer , self.heartbeatTimeOut)
            self.tHeartbeat.start()
    
    def selectEntries(self,n):
        """Selects which entries should be sent to node n"""
        list = []
        term=None
        prevLog= self.nextIndex[n] - 1
        if self.nextIndex[n]>0 and len(self.logs)>0:
            logging.debug("len logs: %s | prevlog: %s",len(self.logs),prevLog)
            prevTerm= self.logs[prevLog][3]
        else:
            prevTerm=None

        if self.nextIndex[n] <= self.lastAppended:
            term=self.logs[self.nextIndex[n]][3]
            for i in range(self.nextIndex[n],self.lastAppended+1):
                #if term != self.logs[i][2]: break
                list.append(self.logs[i])



        return list,prevLog,prevTerm

    def commitEntry(self,commit):
        if self.lastAppended >= commit:
            self.commitIndex=commit
            self.kv[self.logs[self.commitIndex][0]]=self.logs[self.commitIndex][1]


    def mainCycle(self):
        self.tElection = threading.Timer(self.electionTimer , self.electionTimeOut)
        self.tElection.start()

        for msg in receiveAll():
            logging.info("Recebi mensagem! Refresh tElection")
            if msg.body.type == "redirect":
                msg = msg.body.value
            messageid = msg.body.msg_id
            mtype = msg.body.type

            # If it is a client message
            if mtype in ["init","read","write","cas","q_read","q_read_ok"]:
                match mtype:
                    case "init":
                        self.node_id = msg.body.node_id
                        #self.leader = self.node_id
                        self.neighbours = msg.body.node_ids
                        self.majority = len(self.neighbours)/2 +1
                        self.neighbours.remove(self.node_id)
                        for n in self.neighbours:
                            self.nextIndex[n]=0
                        
                        logging.info('node %s initialized', self.node_id)
                        reply(msg, type='init_ok')
                        
                    case "read":
                        key = msg.body.key
                        value = self.kv[key] if key in self.kv else None

                        if self.imLeader():
                            if not value:
                                reply(msg,type="error", code=20)
                            else:
                                reply(msg,type="read_ok", value=value)
                        else:
                            logging.debug("Read request to a follower redirect! To NodeID: %s",self.leader)
                            for i in self.neighbours:
                                if i != self.node_id:
                                    send(self.node_id,i,type="q_read",key=key,id=messageid)
                            logging.debug(value)
                            self.q_buffer[messageid] = (1,{value:1},msg) 

                    case "write":
                        if self.imLeader():
                            key = msg.body.key
                            value = msg.body.value
                            logging.debug("Write request to leader! KeyValue:%s | NodeID:%s",(key,value),self.node_id)

                            # Store client's message
                            self.messages[messageid] = msg

                            # Create new log
                            logEntry = (key,value,messageid,self.current_term)

                            #Append new entry
                            self.lastAppended+=1 
                            self.logs.append(logEntry)     

                            # Distribute append entry
                            for n in self.neighbours:
                                entries,prevLog,prevLogTerm=self.selectEntries(n)

                                send(self.node_id,
                                    n,
                                    type        = "append_entries",
                                    term        = self.current_term,
                                    commitedLogs= self.commitIndex,
                                    leaderId    = self.node_id,
                                    entries     = entries,
                                    prevLog     = prevLog,
                                    prevLogTerm = prevLogTerm,
                                    appendIndex = self.lastAppended)
                
                                if (self.nextIndex[n]==self.lastAppended):
                                    self.nextIndex[n]+=len(self.selectEntries(n)[0])

                        else:
                            logging.debug("Write request to a follower redirect! To NodeID:%s",self.leader)
                            if self.leader != None:send(self.node_id,self.leader,value=msg,type="redirect")
                    
                    case "cas":
                        #logging.info("compare %s to %s %s",msg.body.key,msg.body["from"],msg.body.to)
                        if self.imLeader():
                            key = msg.body.key


                            if(key in self.kv):
                                if (self.kv[key] == getattr(msg.body,"from")): 
                                    #Stores msg
                                    self.messages[messageid] = msg

                                    #Create new logEntrie
                                    logEntry = (key,msg.body.to,messageid,self.current_term)

                                    #Append new entry
                                    self.lastAppended+=1 
                                    self.logs.append(logEntry)

                                    # Distribute append entry
                                    for n in self.neighbours:
                                        entries,prevLog,prevLogTerm=self.selectEntries(n)

                                        send(self.node_id,
                                            n,
                                            type        = "append_entries",
                                            term        = self.current_term,
                                            commitedLogs= self.commitIndex,
                                            leaderId    = self.node_id,
                                            entries     = entries,
                                            prevLog     = prevLog,
                                            prevLogTerm = prevLogTerm,
                                            appendIndex = self.lastAppended)
                        
                                        if (self.nextIndex[n]==self.lastAppended):
                                            self.nextIndex[n]+=len(self.selectEntries(n)[0])


                                else:
                                    reply(msg,type="error",code=22)
                            else:
                                reply(msg,type="error",code=20)
                        else:
                            logging.debug("Cas request to a follower redirect! To NodeID:%s",self.leader)
                            if self.leader != None:send(self.node_id,self.leader,value=msg,type="redirect")

                    case "q_read":
                        key = msg.body.key
                        value = self.kv[key] if key in self.kv else None
                        reply(msg,type="q_read_ok", value=value,id=msg.body.id)

                    case "q_read_ok":
                        if msg.body.id in self.q_buffer:
                            value = msg.body.value
                            num,dic,r_msg = self.q_buffer[msg.body.id]
                            num += 1

                            logging.debug(value)

                            if value in dic:
                                dic[value] += 1
                            else:
                                dic[value] = 1

                            if dic[value] >= self.majority:
                                if value == None:
                                    reply(r_msg,type="error", code=20)
                                else:
                                    reply(r_msg,type="read_ok", value=value)
                                del self.q_buffer[msg.body.id]

                            elif num == len(self.neighbours): # In case a majoraty didnt happend
                                del self.q_buffer[msg.body.id]

                            else:
                                self.q_buffer[msg.body.id] = (num,dic,r_msg)


            # Basic non-client message treatment
            else:
                msg_commit = msg.body.commitedLogs
                msg_term = msg.body.term

                # If msg has a current term higher than mine
                if self.current_term < msg_term: 
                    if self.commitIndex <= msg_commit:
                        if self.tHeartbeat != None:
                            logging.debug("step down")
                            self.tHeartbeat.cancel()
                            self.tHeartbeat.join()
                            self.tHeartbeat.cancel()
                            self.tHeartbeat=None
                        self.current_term = msg_term
                        self.voted_for = None
                    else:
                        mtype="ignore_msg"
                    
                
                elif self.current_term > msg_term:
                    if self.commitIndex < msg_commit:
                        if self.tHeartbeat != None:
                            logging.debug("step down")
                            self.tHeartbeat.cancel()
                            self.tHeartbeat.join()
                            self.tHeartbeat.cancel()
                            self.tHeartbeat=None
                        self.current_term = msg_term
                        self.voted_for = None
                    else:
                        #Ignoras a mensagem 
                        mtype="ignore_msg"
                    #Envia msg ou torna-te candidato
                
                if self.commitIndex < msg_commit and mtype!="ignore_msg":    
                    self.commitEntry(msg_commit)

                if mtype!="ignore_msg": 
                    # Restart election timer
                    self.tElection.cancel()
                    self.tElection.join(0)

                match mtype:
                    case "append_entries":
                        logging.info("Received an append_entries request! NodeID: %s",self.node_id)
                        leadersTerm = msg.body.term
                        entries = msg.body.entries
                        prevLogLeader = msg.body.prevLog
                        prevTermLeader = msg.body.prevLogTerm
                        appendIndexLeader = msg.body.appendIndex
                        
                        sucess = True
                        self.leader = msg.src
                        # If it is not a heartbeat
                        if len(entries)> 0: 
                            if leadersTerm == self.current_term :
                                
                                for e in entries:
                                    if self.lastAppended < prevLogLeader:
                                        sucess=False

                                    elif self.lastAppended == prevLogLeader and prevLogLeader == -1:
                                        self.logs.append(e)
                                        self.lastAppended+=1  
                                        prevTermLeader= e[3]
                                        prevLogLeader+=1

                                    elif self.lastAppended == prevLogLeader and  self.logs[prevLogLeader][3] == prevTermLeader:
                                        self.logs.append(e)
                                        self.lastAppended+=1  
                                        prevTermLeader= e[3]
                                        prevLogLeader+=1
                                    
                                    elif self.lastAppended == prevLogLeader and self.logs[prevLogLeader][3] != prevTermLeader:
                                        sucess=False
                                
                                    elif self.lastAppended > prevLogLeader: 
                                        # Clear all in from and replace the current one
                                        for i in range(appendIndexLeader,len(self.logs)): 
                                            if len(self.logs)>0: self.logs.pop(appendIndexLeader)
                                        
                                        #TODO Falta verificar o termo
                                        self.logs.append(e)
                                        self.lastAppended=appendIndexLeader
                                        prevTermLeader= e[3]
                                        prevLogLeader+=1                     
                                    
                            send(self.node_id,msg.src,
                                type="append_reply",
                                term = self.current_term,
                                commitedLogs=self.commitIndex,
                                lastAppended = self.lastAppended,
                                sucess=sucess)
                    
                    case "append_reply":
                        if msg.body.sucess:
                            self.matchIndex[msg.src] = self.lastAppended
                            #self.nextIndex[msg.src]= self.lastAppended +1
                            #TODO: Rever commit votes
                            if self.lastAppended > self.commitIndex:
                                commitVotes =1
                                for i in self.matchIndex.keys():
                                    if self.matchIndex[i]==self.lastAppended:
                                        commitVotes+=1
                                if commitVotes >= self.majority:
                                    #Start commit process
                                    self.commitIndex+=1
                                    self.kv[self.logs[self.commitIndex][0]]=self.logs[self.commitIndex][1]
                                    rep_msg = self.messages[self.logs[self.commitIndex][2]]
                                    rep_type = "write_ok" if rep_msg.body.type == "write" else "cas_ok"
                                    reply(rep_msg, type=rep_type)
                                    for n in self.neighbours:
                                        send(self.node_id,n,
                                            type="commit_entries",
                                            term=self.current_term,
                                            commitedLogs= self.commitIndex,
                                            leaderID=self.node_id)
                        else:
                            #AppendEntries fails because of log inconsistency: decrement nextIndex and retry
                            #TODO Improvement needed
                            self.nextIndex[msg.src] = msg.body.lastAppended
                            pass

                    case "commit_entries":
                        commitLog = msg.body.commitedLogs
                        self.commitEntry(commitLog)
                                
                    case "vote_request":
                        if self.current_term <= msg.body.term and (self.voted_for==None or self.voted_for == msg.src) and (msg.body.lastLogIndex >= self.lastAppended):    
                            if (len(self.logs)>0 and self.logs[msg.body.lastLogIndex][3] <= msg.body.lastLogTerm) or len(self.logs)==0:
                                self.voted_for=msg.src
                                send(self.node_id,msg.src,type="vote_response",term=self.current_term,commitedLogs=self.commitIndex,vote=True)
                            else:
                                send(self.node_id,msg.src,type="vote_response",term=self.current_term,commitedLogs=self.commitIndex,vote=False)
                        else:
                            send(self.node_id,msg.src,type="vote_response",term=self.current_term,commitedLogs=self.commitIndex,vote=False)

                    case "vote_response":
                        if msg.body.vote == True:
                            self.candidateCounter+=1
                            self.votesReceived+=1
                            logging.info("Received one vote :" + str(self.candidateCounter))
                            if self.candidateCounter>=self.majority:
                                #become lider
                                logging.info("I AM THE KING :" + self.node_id)
                                for n in self.neighbours:
                                    self.nextIndex[n]=self.lastAppended+1

                                self.tElection.cancel()
                                self.tElection.join(0)
                                self.tHeartbeat = threading.Timer(self.heartBeatTimer , self.heartbeatTimeOut)
                                self.tHeartbeat.start()
                            elif self.votesReceived == len(self.neighbours) + 1:
                                self.voted_for=None
            
                if not self.imLeader() and mtype!="ignore_msg":
                    self.tElection = threading.Timer(self.electionTimer , self.electionTimeOut)
                    self.tElection.start()
                        

r = Raft()
r.mainCycle()




