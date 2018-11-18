from bitstring import BitArray
from pyblake2 import blake2b

# This is the base threshold
NANO_DIFFICULTY = 0xffffffc000000000

# Banano is lower
BANANO_DIFFICULTY = 0xfffffe0000000000


def to_multiplier(base_difficulty: int, difficulty: int) -> float:
    return float((1 << 64) - base_difficulty) / float((1 << 64) - difficulty)


def from_multiplier(base_difficulty: int, multiplier: float) -> int:
    return int((base_difficulty - (1 << 64)) / multiplier + (1 << 64))


# tests
assert(to_multiplier(NANO_DIFFICULTY, BANANO_DIFFICULTY) == 0.125)
assert(from_multiplier(NANO_DIFFICULTY, 0.125) == BANANO_DIFFICULTY)


def threshold_to_str(th):
    return hex(th)[2:]


def threshold_from_str(s):
    return int('0x'+s, 16)


def hex_to_account(hex_acc: str) -> (str, str):
    """
    Given a string containing a hex address, encode to public address.
    :return string public account address as xrb_(...)
    :param hex_acc: string encoded address
    # TODO consider nano_(...) addresses
    """

    # format with checksum
    # each index = binary value, account_lookup['00001'] == '3'
    account_map = "13456789abcdefghijkmnopqrstuwxyz"
    account_lookup = {}
    # populate lookup index for binary string to base-32 string character
    for i in range(32):
        account_lookup[BitArray(uint=i, length=5).bin] = account_map[i]
    # hex string > binary
    account = BitArray(hex=hex_acc)

    # get checksum
    h = blake2b(digest_size=5)
    h.update(account.bytes)
    checksum = BitArray(hex=h.hexdigest())

    # encode checksum
    # swap bytes for compatibility with original implementation
    checksum.byteswap()
    encode_check = ''
    for x in range(0, int(len(checksum.bin) / 5)):
        # each 5-bit sequence = a base-32 character from account_map
        encode_check += account_lookup[checksum.bin[x * 5:x * 5 + 5]]

    # encode account
    encode_account = ''
    while len(account.bin) < 260:
        # pad our binary value so it is 260 bits long before conversion
        # (first value can only be 00000 '1' or 00001 '3')
        account = '0b0' + account
    for x in range(0, int(len(account.bin) / 5)):
        # each 5-bit sequence = a base-32 character from account_map
        encode_account += account_lookup[account.bin[x * 5:x * 5 + 5]]

    # build the full address
    return 'xrb_{}{}'.format(encode_account, encode_check)
