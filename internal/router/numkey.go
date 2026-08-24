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
// canonicalKeyValue rewrites a numeric value that is a whole number into one
// form. Whole numbers are what this actually happens to: a driver sends a
// JavaScript number as a double, an older writer or a migration wrote a long,
// and the two are the same account number.
//
// Fractional values are left alone. A double and a decimal that both read as
// 0.1 are not the same number, and deciding which of them mongod would call
// equal means reproducing its comparison rather than sidestepping it. That case
// is no worse than it was, and it is not the one that happens.
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
		// The bounds are the ones a float64 can hold exactly at that
		// magnitude, so the conversion below cannot overflow. NaN fails the
		// first test, because it equals nothing including itself.
		if ok && d == math.Trunc(d) && d >= -9223372036854775808.0 && d < 9223372036854775808.0 {
			return int64(d)
		}
	case bson.TypeDecimal128:
		if d, ok := rv.Decimal128OK(); ok {
			if i, ok := decimalWholeNumber(d); ok {
				return i
			}
		}
	}
	return v
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
