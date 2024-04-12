# node_sys is in charge of encapsulating application behavior
# master node handles communicating with all parties via its own CommSystem, it also has access to its own data via its DataSystem and the slave nodes
# slave driver exists on the master node and serves to function as if the master node can access physical slave node data via the same calls as a DataSystem
# slave node controller exists on the physical slave node and receives commands from the slave data on the master node to drive the behavior of its component Data and CommSystems

# operation:
#   the master node receives a command and operates on its own dataset as well as its slaves, terminating early on an OK
#   any data returned from a slave dataset will be removed from the slave data and re-cached on the master dataset, regardless of whether it was used or not

# proof:
#   note that the caching approach can be guaranteed to work iff caching occurs on the FIRST request for the data. 
#   though the currency data could grow to hilarious bounds, it should be noted that as long as no GIVE commands have yet occurred, the currency data
#   remains in the initial bounds

from enum import Enum
import sys
import time
from comms_sys import CommSystem
from cs145lib.task2.utils import Employee
from cs145lib.task2 import Channel
from data_sys import DataStatus, DataSystem
from typing import List, Sequence, Tuple
from status import ControlStatus, CommStatus, SybilStatus

# this class is a node subsystem that will use data gathered from MasterNode's results to predict the results of future queries without needing to refer to 
#   actual data sources
class SybilSystem:
  
  record: dict[int, SybilStatus]
  control: ControlStatus
  
  def __init__(self) -> None:
    
    # initialize records as GREY
    self.record = dict([(i, SybilStatus.GREY) for i in range(1, 65535 + 1)])
    
  def mark_white(self, id: int):
    self.record[id] = SybilStatus.WHTE
    
  def mark_black(self, id: int):
    self.record[id] = SybilStatus.BLCK
    
  def set_control(self, new_control: ControlStatus):
    self.control = new_control
    
  # interpret the results of a given command on an id
  def interpret_result(self, controlStatus: ControlStatus, dataStatus: DataStatus, id: int, bound: int | None):
    return self.interpret_give(id) if self.control == ControlStatus.GIVE else self.interpret_query(id)
  
  def interpret_give(self, dataStatus: DataStatus, id: int, bounds: int | None):
    match DataStatus:
      
      # mark the id white
      case DataStatus.CASE1:
        self.mark_white(id)
        
      # mark [id, bounds] black
      case DataStatus.CASE2:
        self.mark_white(bounds)
        [self.mark_black(i) for i in range(id, bounds)]
        
      # mark [id, id + 100] black and mark [bounds + 1, id] black
      case DataStatus.CASE3:
        self.mark_white(bounds)
        [self.mark_black(i) for i in range(id, 100 + 1)]
        [self.mark_black(i) for i in range(bounds + 1, id)]
        
      case DataStatus.FAIL:
        [self.mark_black(i) for i in range(id, 100 + 1)]
        [self.mark_black(i) for i in range(id - 100, id)]
  
  def interpret_query(self, dataStatus: DataStatus, id: int):
    match DataStatus:
      case DataStatus.OK:
        self.mark_white(id)
      case DataStatus.FAIL:
        self.mark_black(id)
  
  # predict the results of a given command on a given id (FALSE = not worth trying, do not proceed; TRUE = worth trying, proceed)
  def predict_id(self, id: int) -> bool:
    return self.predict_give(id) if self.control == ControlStatus.GIVE else self.predict_query(id)
  
  # check if ANY value in the checking range is not black, if so proceed; if not preemptively fail
  def predict_give(self, id: int) -> bool:
    return any([self.record[i] != SybilStatus.BLCK for i in range(id - 100, id + 100 + 1)])
  
  # check if the id itself is black
  def predict_query(self, id: int) -> bool:
    return self.record[id] == SybilStatus.BLCK


class MasterNode:

  comms: CommSystem
  sources: List[DataSystem]
  control: ControlStatus
  sybil: SybilSystem

  # we assume sources[0] is the master source
  def __init__(self, chan: Channel, sources: List[DataSystem]) -> None:
    self.comms = CommSystem(chan, CommStatus.SUB, CommStatus.CMD)
    self.sources = sources
    self.sybil = SybilSystem()
  
  # this method starts the master node operation loop
  def operate(self):
    for operation_no in range(1, 1000 + 1):
      cmd = self.comms.receive()
      assert isinstance(cmd, Tuple)

      self.set_control(cmd[0])
      self.comms.set_control(cmd[0])
      self.sybil.set_control(cmd[0])
      
      print(f"### OPERATION {operation_no}: {cmd} ###", file=sys.stderr)
      
      result = self.search_sources(cmd, operation_no)
      
      # if the control status is give, give the money to the cached data PROVIDED that the operation did not fail
      if self.control == ControlStatus.GIVE and result[0] != DataStatus.FAIL:
        print(f"#:{operation_no} giving {result[1]} {cmd[2]}, with bal {self.sources[0].query(result[1])} for {result[1]}")
        self.sources[0].give(result[0], result[1], cmd[2])
        print(f"  new bal is {self.sources[0].query(result[1])} for {result[1]}")
      
      self.comms.send(result)
      
    # upon completion of operations, terminate all DataSystems
    [ds.terminate() for ds in self.sources]
    
    return
  
  def search_sources(self, cmd: Tuple, operation_no: int) -> Tuple:
    
    result: Tuple = (DataStatus.FAIL, )
    
    # check every source, terminate if there's an exact match
    for source in self.sources:
      
      temp = source.find(cmd[1]) if self.control == ControlStatus.GIVE else source.query(cmd[1])
      
      # if the result from a slave node was not a FAIL, cache the result in the data since we destroy the original slave data no matter what
      if source != self.sources[0] and temp[0] != DataStatus.FAIL:
        if self.control == ControlStatus.GIVE:
          self.cache_record(temp[1], temp[2])
        else:
          self.cache_record(cmd[1], temp[1])
      
      print(f"#:{operation_no} source {source} found {temp}")
      
      assert isinstance(temp, Tuple)
      assert isinstance(result, Tuple)
      
      # if the temporary result is already OK, accept it and carry on
      if (temp[0] == DataStatus.CASE1 and self.control == ControlStatus.GIVE) or (temp[0] == DataStatus.OK and self.control == ControlStatus.QUERY):
        result = temp
        break
      
      # if the temporary result is 'better' than the current result (*both of them must be CASEx results*)
      if temp[0] in range(30, 35) and result[0] in range(30, 35):
        result = temp if temp[0].value < result[0].value else result
      
      result = self.better_result(result, temp)
      
    return result
  
  # return the best result tuple from the input result tuples
  def better_result(self, tup1: Tuple, tup2: Tuple) -> Tuple:
    
    print(f"comparing {tup1} and {tup2}")
    
    # if they are not equal in case level, return the smaller valued tuple
    if tup1[0] != tup2[0]:
      print(f"comparing {tup1[0].value} and {tup2[0].value}")
      return min([tup1, tup2], key=lambda x: x[0].value)
    
    match tup1[0]:
      
      # if both are CASE2, return the smaller 
      case DataStatus.CASE2:
        return min([tup1, tup2], key=lambda x: x[1])
      
      # if both are CASE3, return the bigger
      case DataStatus.CASE3:
        return max([tup1, tup2], key=lambda x: x[1])
      
      # if both are FAIL, we don't really care
      case DataStatus.FAIL:
        return tup1
    
  # provided data about a slave record, cache it into our own data
  def cache_record(self, id: int, p: int):
    print(f"caching new record (id:{id}, p:{p})")
    self.sources[0].set(id, p)
    return (id, p)
      
  def set_control(self, new_control: ControlStatus):
    self.control = new_control


class SlaveDriver(DataSystem):

  comms: CommSystem
  control: ControlStatus

  def __init__(self, chan: Channel) -> None:
    self.comms = CommSystem(chan, CommStatus.CMD, CommStatus.DAT)

  # drive the node to execute the find command
  def find(self, id: int) -> Tuple[DataStatus] | Tuple[DataStatus, int, int]:
    self.comms.set_control(ControlStatus.FIND)
    self.comms.send( (ControlStatus.FIND, id) )
    return self.comms.receive()

  # drive the node to execute the query command
  def query(self, id: int) -> Tuple[DataStatus] | Tuple[DataStatus, int]:
    self.comms.set_control(ControlStatus.QUERY)
    self.comms.send( (ControlStatus.QUERY, id) )
    return self.comms.receive()
    
  # terminate the driven slave node
  def terminate(self):
    self.comms.send( (ControlStatus.TERM, 0) )


class SlaveNode:

  comms: CommSystem
  data: DataSystem
  control: ControlStatus

  def __init__(self, data: Sequence[Employee], chan: Channel) -> None:
    self.comms = CommSystem(chan, CommStatus.DAT, CommStatus.CMD)
    self.data = DataSystem(data)

  # this method starts the slave node operation loop
  def operate(self):
    while True:
      cmd = self.comms.receive()
      
      assert isinstance(cmd, Tuple)
      
      # set controls for children
      self.set_control(cmd[0])
      self.comms.set_control(cmd[0])
      
      target_id = cmd[1]
      result: Tuple = ()
      
      # determine the operation to be executed and execute it
      match self.control:
        case ControlStatus.QUERY:
          result = self.data.query(target_id)
        case ControlStatus.FIND:
          result = self.data.find(target_id)
        case ControlStatus.TERM:
          return
        
      print(f"slave found {result}", file=sys.stderr)
          
      # wipe the slave data, if it exists
      if result[0] != DataStatus.FAIL:
        clear_id = result[1] if self.control == ControlStatus.FIND else target_id
        self.data.clear(clear_id)
      
      # send the result of the operation
      self.comms.send(result)
          
  def set_control(self, new_control: ControlStatus):
    self.control = new_control
