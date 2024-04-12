# this class handles the manipulation of data within a node

from collections.abc import Sequence
from math import floor
import sys
from cs145lib.task2 import Employee
from typing import Tuple
from status import DataStatus

class DataSystem:
  
  data: dict[int, int] = dict()

  def __init__(self, data: Sequence[Employee]) -> None:
    self.data = dict([(emp.id, emp.balance) for emp in data])

  # try and retrieve the nearest EmployeeID within range of bound
  def try_retrieve(self, employee_id: int, bound: int) -> int | None:
    for i in range(abs(bound) + 1):
        try_id = employee_id + (-1 if bound < 0 else 1) * i
        if self.data.get(try_id, None) != None:
            return try_id
    return None 

  # find the employee with id 'id' or the approximation thereof and return the (status, id, pesos) of the entry, otherwise FAIL
  def find(self, employee_id: int) -> Tuple[DataStatus] | Tuple[DataStatus, int]:
    target = None
    ret = DataStatus.FAIL
    
    # try to get the data value at id 
    if (target := employee_id if self.data.get(employee_id, None) != None else None) != None:
        ret = DataStatus.CASE1
    elif (target := self.try_retrieve(employee_id, 100)) != None:
        ret = DataStatus.CASE2
    elif (target := self.try_retrieve(employee_id, -100)) != None:
        ret = DataStatus.CASE3

    if ret == DataStatus.FAIL:
        return (ret, )
    
    return (ret, target if target != None else -1, self.data[target])

  # give the employee with id 'id' 'p' pesos
  def give(self, case: DataStatus, employee_id: int, p: int):
    target_p : int
    
    match case:
      case DataStatus.CASE2:
        target_p = floor(p / 2)
      case DataStatus.CASE3:
        target_p = floor(p / 3)
      case _:
        target_p = p
    
    self.data[employee_id] += target_p
    
  def set(self, employee_id: int, p: int):
    self.data[employee_id] = p

  # query the employee with id 'id' and return OK, p for p however many pesos the id has or FAIL if 'id' is not found
  def query(self, employee_id: int) -> Tuple[DataStatus] | Tuple[DataStatus, int]:
    ret = self.data.get(employee_id, None)
    return (DataStatus.OK, ret) if ret != None else (DataStatus.FAIL, )

  # clear the entry associated with the id
  def clear(self, employee_id: int):
    print(f"destroying record for id:{employee_id}")
    return self.data.pop(employee_id)
  
  # end operation, useless for DataSystem
  def terminate(self):
    pass