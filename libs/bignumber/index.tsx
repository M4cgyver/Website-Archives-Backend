export function hexToBn(hex: string, options?: { unsigned?: boolean }): bigint {
    //console.trace(hex);

    const unsigned = options?.unsigned ?? false;

    if (hex.length % 2 || hex.length === 0) {
        hex = '0' + hex;
    }

    const bn = BigInt('0x' + hex);

    if (unsigned) {
        return bn;
    }

    // Check if the highest bit of the highest byte is set (sign bit)
    const highByte = parseInt(hex.slice(0, 2), 16);
    if (highByte >= 128) {
        // Calculate two's complement (flip bits, add one)
        const flipped = bn ^ ((1n << BigInt(hex.length * 4)) - 1n);
        return -flipped - 1n;
    }

    return bn;
}
