package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Duration is a time.Duration that also understands days in YAML.
//
// Retention is discussed in days ("keep 90 days in the fast tier"), and
// spelling that as 2160h in a config file invites arithmetic mistakes during an
// incident. Everything Go's ParseDuration accepts still works.
type Duration time.Duration

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) String() string {
	if d == 0 {
		return "0s"
	}
	if rem := time.Duration(d) % (24 * time.Hour); rem == 0 {
		return strconv.Itoa(int(time.Duration(d)/(24*time.Hour))) + "d"
	}
	return time.Duration(d).String()
}

// UnmarshalYAML parses "90d", "36h", "30s" and the like.
func (d *Duration) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	parsed, err := ParseDuration(s)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

// ParseDuration extends time.ParseDuration with a day unit. Days are exactly
// 24 hours here; this is a retention knob, not a calendar.
func ParseDuration(s string) (Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	if days, ok := strings.CutSuffix(s, "d"); ok {
		// Reject "1.5d0m" style input by requiring the whole prefix to be a number.
		n, err := strconv.ParseFloat(days, 64)
		if err != nil {
			return 0, fmt.Errorf("parse duration %q: %w", s, err)
		}
		return Duration(float64(24*time.Hour) * n), nil
	}

	v, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("parse duration %q: %w", s, err)
	}
	return Duration(v), nil
}
