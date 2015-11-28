

def target_to_bits(target):
    """Convert full target to compact target.

    Args:
        target (int): Full target.

    Returns:
        int: Compact target.

    """
    # Get bit length
    nbits = target.bit_length()
    # Round up to next 8-bits
    nbits = ((nbits + 7) & ~0x7)
    exponent = (int(nbits / 8) & 0xff)
    coefficient = (target >> (nbits - 24)) & 0xffffff
    if coefficient & 0x800000:
        coefficient >>= 8
        exponent += 1
    return (exponent << 24) | coefficient


def bits_to_target(bits):
    """Convert compact target to full target.

    Args:
        bits (int): Compact target.

    Returns:
        int: Full target.

    """
    exponent = ((bits >> 24) & 0xff)
    coefficient = (bits & 0x7fffff)
    if bits & 0x800000:
        # https://bitcoin.org/en/developer-reference#target-nbits
        return 0x0
    return coefficient * 2 ** (8 * (exponent - 3))


def difficulty_to_target(difficulty):
    """Convert difficulty to full target.

    Args:
        difficulty (float): Difficulty.

    Returns:
        int: Full target.

    """
    return int(0x00000000FFFF0000000000000000000000000000000000000000000000000000 / difficulty)


def target_to_difficulty(target):
    """Convert full target to difficulty.

    Args:
        target (int): Full target.

    Returns:
        float: Difficulty.

    """
    return 0x00000000FFFF0000000000000000000000000000000000000000000000000000 / target


def difficulty_to_bits(difficulty):
    """Convert difficulty to compact target.

    Args:
        difficulty (float): Difficulty.

    Returns:
        int: Compact target.

    """
    return target_to_bits(difficulty_to_target(difficulty))


def bits_to_difficulty(bits):
    """Convert compact target to difficulty.

    Args:
        bits (int): Compact target.

    Returns:
        float: Difficulty.

    """
    return target_to_difficulty(bits_to_target(bits))
