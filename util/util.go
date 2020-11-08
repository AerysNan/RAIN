package util

// FiniteFieldMultiply compute the multiplication of two finite field numbers
func FiniteFieldMultiply(x byte, y byte, generator int16) byte {
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

// FiniteFieldMultiply2 compute the multiplication of two finite field numbers
func FiniteFieldMultiply2(x int16, y int16, generator int16) int16 {
	m, n, v, mask := x, y, int16(0), int16(1)
	for i := 0; i < 8; i++ {
		if (mask & m) > 0 {
			v = v ^ n
		}
		n <<= 1
		mask <<= 1
	}
	_, result := FiniteFieldDivide2(v, generator)
	return result
}

// FiniteFieldDivide compute the division of two finite field numbers
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

// FiniteFieldDivide2 compute the division of two finite field numbers
func FiniteFieldDivide2(x int16, y int16) (int16, int16) {
	xDegree, result := 15, 0
	for ; xDegree > 0; xDegree-- {
		if (x & (1 << xDegree)) > 0 {
			break
		}
	}

	yDegree := 15
	for ; yDegree > 0; yDegree-- {
		if (y & (1 << yDegree)) > 0 {
			break
		}
	}

	mask := int16(1 << xDegree)
	for i := xDegree; i >= yDegree; i-- {
		if (mask & x) > 0 {
			result ^= 1 << (i - yDegree)
			x ^= y << (i - yDegree)
		}
		mask >>= 1
	}
	return int16(result), x
}

// FiniteFieldExtendedEuclideanRecursive perform EE algorithm in a recursive manner
func FiniteFieldExtendedEuclideanRecursive(x int16, y int16, generator int16) (int16, int16) {
	if y == 0 {
		return 1, 0
	}
	result, remainder := FiniteFieldDivide2(x, y)
	a, b := FiniteFieldExtendedEuclideanRecursive(y, remainder, generator)
	return b, a ^ FiniteFieldMultiply2(result, b, generator)
}

// FiniteFieldExtendedEuclideanIterative perform EE algorithm in a iterative manner
func FiniteFieldExtendedEuclideanIterative(x int16, y int16, generator int16) (int16, int16) {
	a, b := int16(1), int16(0)
	for y != 0 {
		result, remainder := FiniteFieldDivide2(x, y)
		x, y = y, remainder
		a, b = b, a^FiniteFieldMultiply2(result, b, generator)
	}
	return a, b
}

// FiniteFieldInvert compte the inversion of a finite field number
func FiniteFieldInvert(x int16, generator int16, isIterative bool) int16 {
	y := generator
	if isIterative {
		result, _ := FiniteFieldExtendedEuclideanIterative(x, y, generator)
		return result
	}
	result, _ := FiniteFieldExtendedEuclideanRecursive(x, y, generator)
	return result
}
