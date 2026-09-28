package router

import (
	"math"
	"math/big"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// MongoDB makes one unique key out of the same number written three ways. It
// compares int, long, double and decimal against each other, so a collection
// holding NumberLong(7) rejects an insert of 7.
//
// The registry encodes a key by marshalling the value, where those four have
// different bytes, so it made four keys where mongod makes one. Only the cross
// tier case can reach it: within the fast tier mongod's own index still refuses
// the duplicate. Measured on a tiered collection whose archive held
// NumberLong(7), the router accepted an insert of 7 that mongod rejects.
//
// canonicalKeyValue rewrites a numeric value into one form per number, so the
// same number written as an int, a long, a double or a decimal makes one key,
// as it does in mongod.
//
// Whole numbers that fit an int64 become that int64. Every other number
// becomes the decimal that holds its exact value, when one does, and stays as
// it is when none does. That is mongod's rule, measured on 2026-09-27 with a
// unique index on a plain mongod: double 0.5 and NumberDecimal("0.5000") are
// one key, because both are exactly one half, while double 0.1 and
// NumberDecimal("0.1") are two, because the double is not exactly one tenth,
// and neither is 1e300 the same key as NumberDecimal("1E+300"). Every NaN is
// one key and so is each infinity, whatever the type.
//
// Before this only whole numbers were rewritten, and the router accepted an
// insert of NumberDecimal("0.5"), NumberDecimal("2.250"), a decimal Infinity
// and a decimal NaN into collections whose archive held 0.5, 2.25, Infinity
// and NaN as doubles, each a duplicate mongod refuses.
func canonicalKeyValue(v any) any {
	rv, ok := v.(bson.RawValue)
	if !ok {
		return v
	}
	switch rv.Type {
	case bson.TypeInt32:
		i, ok := rv.Int32OK()
		if ok {
			return int64(i)
		}
	case bson.TypeInt64:
		i, ok := rv.Int64OK()
		if ok {
			return i
		}
	case bson.TypeDouble:
		d, ok := rv.DoubleOK()
		if !ok {
			return v
		}
		switch {
		case math.IsNaN(d):
			return canonicalNaN
		case math.IsInf(d, 1):
			return canonicalInf
		case math.IsInf(d, -1):
			return canonicalNegInf
		}
		// The bounds are the ones a float64 can hold exactly at that
		// magnitude, so the conversion below cannot overflow.
		if d == math.Trunc(d) && d >= -9223372036854775808.0 && d < 9223372036854775808.0 {
			return int64(d)
		}
		if dec, ok := exactDecimal(d); ok {
			return dec
		}
	case bson.TypeDecimal128:
		d, ok := rv.Decimal128OK()
		if !ok {
			return v
		}
		switch {
		case d.IsNaN():
			return canonicalNaN
		case d.IsInf() > 0:
			return canonicalInf
		case d.IsInf() < 0:
			return canonicalNegInf
		}
		if i, ok := decimalWholeNumber(d); ok {
			return i
		}
		if dec, ok := normalDecimal(d); ok {
			return dec
		}
	}
	return v
}

var (
	canonicalNaN, _    = bson.ParseDecimal128("NaN")
	canonicalInf, _    = bson.ParseDecimal128("Infinity")
	canonicalNegInf, _ = bson.ParseDecimal128("-Infinity")
)

// exactDecimal is the decimal holding exactly the value of a finite double,
// when a decimal128 can hold it: at most 34 significant digits.
//
// A double is a fraction whose denominator is a power of two, 2^k, and
// n/2^k is exactly n*5^k/10^k, so the digits are n*5^k and the exponent -k.
func exactDecimal(d float64) (bson.Decimal128, bool) {
	r := new(big.Rat).SetFloat64(d)
	if r == nil {
		return bson.Decimal128{}, false
	}
	num, den := r.Num(), r.Denom()
	k := den.BitLen() - 1 // den is 2^k
	coef := new(big.Int).Mul(num, new(big.Int).Exp(big.NewInt(5), big.NewInt(int64(k)), nil))
	return fromDigits(coef, -k)
}

// normalDecimal is a decimal with its trailing zeros removed, so 2.250 and
// 2.25, which mongod compares equal, become the same bytes.
func normalDecimal(d bson.Decimal128) (bson.Decimal128, bool) {
	bi, exp, err := d.BigInt()
	if err != nil {
		return bson.Decimal128{}, false
	}
	return fromDigits(bi, exp)
}

// fromDigits builds the decimal coef * 10^exp with no trailing zeros, when it
// fits in 34 digits and decimal128's exponent range.
func fromDigits(coef *big.Int, exp int) (bson.Decimal128, bool) {
	c := new(big.Int).Set(coef)
	ten, rem := big.NewInt(10), new(big.Int)
	for c.Sign() != 0 {
		q, r := new(big.Int).QuoRem(c, ten, rem)
		if r.Sign() != 0 {
			break
		}
		c, exp = q, exp+1
	}
	if len(new(big.Int).Abs(c).String()) > 34 {
		return bson.Decimal128{}, false
	}
	return bson.ParseDecimal128FromBigInt(c, exp)
}

// decimalWholeNumber reads a decimal that is a whole number fitting in an
// int64, which is the only decimal that can share a key with an integer.
//
// A decimal carries its trailing zeros: 7.00 and 7 are stored differently and
// compare equal, so both have to arrive at the same 7 here.
func decimalWholeNumber(d bson.Decimal128) (int64, bool) {
	bi, exp, err := d.BigInt()
	if err != nil {
		return 0, false
	}
	switch {
	case exp > 0:
		bi = new(big.Int).Mul(bi, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil))
	case exp < 0:
		pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-exp)), nil)
		quo, rem := new(big.Int).QuoRem(bi, pow, new(big.Int))
		if rem.Sign() != 0 {
			return 0, false // has a fractional part
		}
		bi = quo
	}
	if !bi.IsInt64() {
		return 0, false
	}
	return bi.Int64(), true
}
