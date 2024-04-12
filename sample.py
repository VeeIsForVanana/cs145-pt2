from collections.abc import Sequence

from cs145lib.task2 import Employee, Channel, node_main


def brandy(people: Sequence[Employee], generoso_ch: Channel, tandy_ch: Channel) -> None:

    # example: reading a frame from Generoso
    frame = generoso_ch.read_frame()
    print(f"HELLO!!! I received this {len(frame)}-byte frame from Generoso:", frame.hex())

    # example: sending a frame to Tandy
    # this sends the bytes [0xC0, 0xFF, 0xEE, 0x60, 0x0D]
    tandy_ch.write_frame(bytes.fromhex('C0FFEE600D'))

    # example: reading a frame from Tandy
    frame = tandy_ch.read_frame()
    print(f"HELLO!!! I received this {len(frame)}-byte frame from Tandy:", frame.hex())

    # example: sending a frame to Generoso
    generoso_ch.write_frame(bytes([0xDE, 0xC0, 0xDE]))

    # example: sending another frame to Generoso
    generoso_ch.write_frame(bytes([240, 13, 250, 206]))


def tandy(people: Sequence[Employee], brandy_ch: Channel) -> None:

    # example: sending a frame to Brandy
    # this sends the bytes corresponding to the ASCII values of these characters
    brandy_ch.write_frame(b'Hi Brandy!')

    # example: reading a frame from Brandy
    frame = brandy_ch.read_frame()
    print(f"HELLO!!! I received this {len(frame)}-byte frame from Brandy:", frame.hex())


if __name__ == '__main__':
    node_main(brandy, tandy)
