from collections.abc import Sequence

from cs145lib.task2 import Employee, Channel, node_main
from comms_sys import CommSystem
from data_sys import DataSystem
from node_sys import MasterNode, SlaveDriver, SlaveNode


def brandy(people: Sequence[Employee], generoso_ch: Channel, tandy_ch: Channel) -> None:
    sources = [DataSystem(people), SlaveDriver(tandy_ch)]
    me = MasterNode(generoso_ch, sources)
    me.operate()


def tandy(people: Sequence[Employee], brandy_ch: Channel) -> None:
    me = SlaveNode(people, brandy_ch)
    me.operate()


if __name__ == '__main__':
    node_main(brandy, tandy)
