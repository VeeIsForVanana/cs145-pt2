from enum import Enum

class DataStatus(Enum):
  OK = 77
  FAIL = 66
  CASE1 = 30
  CASE2 = 31
  CASE3 = 32

class ControlStatus(Enum):
  QUERY = 101
  GIVE = 102
  TERM = 255
  FIND = 201
  
class CommStatus(Enum):
  CMD = 1
  DAT = 2
  SUB = 3
  
