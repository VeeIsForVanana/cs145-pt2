# this class handles the communication of information as frames between systems

import sys
from cs145lib.task2 import Channel
from typing import Tuple
from status import CommStatus, ControlStatus, DataStatus

class CommSystem:
        
    chan: Channel
    
    # how exactly this CommSystem is expected to behave as a sender
    send_behavior: CommStatus

    # how exactly this CommSystem is expected to behave as a receiver
    rcv_behavior: CommStatus
 
    # the notion of a buffer is obsolete, consider this a historical artifact :D
    buffer: bytes
    
    # the control status of the commsys and *supposedly* its owning nodesys
    control: ControlStatus = ControlStatus.GIVE
    
    last_send = bytes([])

    def __init__(self, chan: Channel, send_behavior: CommStatus, rcv_behavior: CommStatus) -> None:
        self.chan = chan 
        self.buffer = bytes([])
        self.send_behavior = send_behavior
        self.rcv_behavior = rcv_behavior
    
    # change the control state of the system, note that by the principles through which the system is designed, CommSys should not by driving its own control state
    #   it should instead rely on its parent system to drive
    def set_control(self, new_control: ControlStatus) -> None:
        self.control = new_control

    def send_translate(self, item: int | DataStatus | ControlStatus | Tuple[DataStatus | ControlStatus, int] | Tuple[DataStatus | ControlStatus, int, int], is_employee_id=False) -> bytes:
        # if the item is an int and id is False (i.e. it is a number representing currency in range(20000, 100000 + 1)), force to 3-bytes in big-endian order
        # if the item is an int and id is True (i.e. it is a number representing an id in range(65535 + 1)), force to 2-bytes in big-endian order
  
        if isinstance(item, int):
            return item.to_bytes(2 if is_employee_id else 3, 'big')
        
        # if the item is a tuple (i.e. it is a DataStatus followed by an int), return each item parsed as bytes
        if isinstance(item, Tuple) and len(item) == 2:
            return b''.join([self.send_translate(item[0]), self.send_translate(item[1], True)])

        if isinstance(item, Tuple) and len(item) == 3:
            return b''.join([self.send_translate(item[0]), self.send_translate(item[1], True), self.send_translate(item[2])])
        
        if isinstance(item, Tuple) and len(item) == 1:
            return  b''.join([self.send_translate(item[0])])
  
        # otherwise (the item is a DataStatus | ControlStatus), return as a single byte
        return item.value.to_bytes(1, 'big')
    
    def send_cmd_translate(self, item: Tuple[ControlStatus, int]): 
        return b''.join([item[0].value.to_bytes(1, 'big'), item[1].to_bytes(2, 'big')])
    
    def send_data_translate(self, item: Tuple[DataStatus] | Tuple[DataStatus, int] | Tuple[DataStatus, int, int]):
        match len(item):
            case 1:
                return  b''.join([self.send_translate(item[0])])
            case 2:
                return b''.join([self.send_translate(item[0]), self.send_translate(item[1])])
            case 3:
                return b''.join([self.send_translate(item[0]), self.send_translate(item[1], True), self.send_translate(item[2])])
    
    # translate an item for submission to lolo
    def submit_translate(self, item: Tuple[DataStatus] | Tuple[DataStatus, int]) -> bytes:
        # no matter what, if FAIL, return FAIL
        if item[0] == DataStatus.FAIL:
            return bytes([DataStatus.FAIL.value])
        
        # if success, proceed along the necessary lane
        if self.control == ControlStatus.GIVE:
            assert isinstance(item[0], DataStatus)
            return bytes([DataStatus.OK.value, item[0].value])
        
        # if the control status is for anything else send the peso value as 8 bytes,
        else:
            assert isinstance(item[1], int)
            as_bytes = item[1].to_bytes(8, 'big')
            return b''.join([bytes([item[0].value]), as_bytes])

    # translate a received byte sequence as a command, note that a control status is optional since the length of the command equivalently encodes for type
    def receive_cmd_translate(self, item: bytes) -> Tuple[ControlStatus, int, int] | Tuple[ControlStatus, int]:
        match len(item):
            # parse a 7 or 6-byte give command structured as follows: STATUS [0] | ID [1, 2] | PESOS [3, 4, 5, 6]
            case 7 | 6:
                return (ControlStatus.GIVE, int.from_bytes(item[1:3], 'big'), int.from_bytes(item[3:], 'big'))
            # parse a 3 or 2-byte query commmand structured as follows: STATUS [0] | ID [1, 2]
            case 3 | 2:
                match item[0]:
                    case ControlStatus.QUERY.value:
                        return (ControlStatus.QUERY, int.from_bytes(item[1:], 'big'))
                    case ControlStatus.FIND.value:
                        return (ControlStatus.FIND, int.from_bytes(item[1:], 'big'))
            case _:
                raise ValueError("what the hell did you feed me")
    
    # translate a received byte sequence as data in a *similar* format to give commands or as a FAIL, note that the DataStatus byte is important
    def receive_data_translate(self, item: bytes) -> Tuple[DataStatus, int, int] | Tuple[DataStatus]:
        match DataStatus(item[0]):
            case DataStatus.FAIL:
                return (DataStatus.FAIL, )
            case _:
                if self.control == ControlStatus.FIND:
                    return (DataStatus(item[0]), int.from_bytes(item[1:3], 'big'), int.from_bytes(item[3:], 'big'))
                else:
                    return (DataStatus(item[0]), int.from_bytes(item[1:], 'big'))

    def _send(self, new_bytes: bytes):
        # we are no longer using the buffered sending approach
        self.last_send = new_bytes
        return self.chan.write_frame(new_bytes)

    def _receive(self) -> bytes:
        rcvd = self.chan.read_frame()
        
        # wait until we stop receiving our last sent bytes
        while rcvd == self.last_send:
            rcvd = self.chan.read_frame()
            
        return rcvd
    
    # send some properly formatted item, return the bytes for debugging
    def send(self, item) -> bytes:
        send_item: bytes = bytes()
        match self.send_behavior:
            case CommStatus.SUB:
                send_item = self.submit_translate(item)
            case CommStatus.DAT:
                send_item = self.send_data_translate(item)
            case CommStatus.CMD:
                send_item = self.send_cmd_translate(item)
        print(f"{'submitting' if self.send_behavior == CommStatus.SUB else 'sending'} {item} as {send_item}", file=sys.stderr)
        self._send(send_item)
        return send_item
    
    # receive some properly formatted item
    def receive(self) -> Tuple:
        rcv_bytes: bytes = self._receive()
        match self.rcv_behavior:
            case CommStatus.CMD:
                rcv_item = self.receive_cmd_translate(rcv_bytes)
            case CommStatus.DAT:
                rcv_item = self.receive_data_translate(rcv_bytes)
        print(f"received {'from god' if self.send_behavior == CommStatus.SUB else ''} {rcv_item} as {rcv_bytes}", file=sys.stderr)
        return rcv_item
    