package util

func FiniteFieldMuiltiply(x byte, y byte, generator int16) byte {
	m, n, v, mask := int16(x), int16(y), int16(0), int16(1)
	for i := 0; i < 8; i++ {
		if (mask & m) > 0 {
			v = v ^ n
		}
		n <<= 1
		mask <<= 1
	}
	_, result := FiniteFieldDivide(v, generator)
	return result
}

func FiniteFieldDivide(x int16, y int16) (byte, byte) {
	degree, result := 15, byte(0)
	for ; degree > 0; degree-- {
		if (x & (1 << degree)) > 0 {
			break
		}
	}
	mask := int16(1 << degree)
	for i := degree; i >= 8; i-- {
		if (mask & x) > 0 {
			result ^= 1 << (i - 8)
			x ^= y << (i - 8)
		}
		mask >>= 1
	}
	return result, byte(x)
}

func FiniteFieldInvert(x byte, generator int16) byte {
	return 0
}
