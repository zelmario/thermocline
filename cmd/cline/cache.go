package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"time"

	"github.com/zelmario/thermocline/internal/config"
	"github.com/zelmario/thermocline/internal/meta"
)

const cacheUsage = `cline cache: which queries would be worth caching, and for how long.

The router watches every read go past and records three things: the shape of the
query, a hash of its parameters, and a hash of what came back. From that it can
say which shapes are asked repeatedly, which of those are asked with repeating
parameters, and how long the same answer kept coming back for the same
parameters. The suggestion is ten per cent inside that: an answer observed to
hold for five minutes is worth caching for four and a half.

Nothing here turns caching on. It reports what turning it on would be worth, so
the decision is made against evidence. Set router.cache.enabled once the numbers
justify it, and the same measurements then drive the TTLs.

Reads what the routers have published, so it needs no access to the routers
themselves.

Options:
`

func runCache(args []string) error {
	fs := flag.NewFlagSet("cache", flag.ExitOnError)
	path := fs.String("config", "/etc/cline.yaml", "path to cline.yaml")
	top := fs.Int("top", 20, "how many queries to show")
	only := fs.String("ns", "", "one namespace as db.collection, default every one seen")
	fs.Usage = func() { fmt.Print(cacheUsage); fs.PrintDefaults() }
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cold, err := connect(ctx, cfg.Cold.URI, "cold")
	if err != nil {
		return err
	}
	defer func() { _ = cold.Disconnect(context.WithoutCancel(ctx)) }()

	reports, err := meta.ReadCacheAdvice(ctx, cold)
	if err != nil {
		return fmt.Errorf("read what the routers published: %w", err)
	}
	if len(reports) == 0 {
		fmt.Println("No router has published anything yet.")
		fmt.Println("A router publishes once a minute while it runs, and once more on the way out.")
		return nil
	}

	for _, rep := range reports {
		age := time.Since(rep.At).Round(time.Second)
		state := "off, measuring only"
		if rep.Enabled {
			total := rep.Hits + rep.Misses
			rate := 0.0
			if total > 0 {
				rate = float64(rep.Hits) / float64(total) * 100
			}
			state = fmt.Sprintf("on, %d hits and %d misses (%.1f%%)", rep.Hits, rep.Misses, rate)
		}
		fmt.Printf("\n%s   %d query shapes seen, cache %s, as of %s ago\n",
			rep.ID, rep.Shapes, state, age)

		rows := rep.Suggestions
		if *only != "" {
			var kept []meta.CacheSuggestion
			for _, s := range rows {
				if s.NS == *only {
					kept = append(kept, s)
				}
			}
			rows = kept
		}
		if len(rows) == 0 {
			fmt.Println("  Nothing repeats often enough yet to be worth caching.")
			continue
		}
		sort.SliceStable(rows, func(i, j int) bool {
			return float64(rows[i].Repeats)*rows[i].Suggested.Seconds() >
				float64(rows[j].Repeats)*rows[j].Suggested.Seconds()
		})
		if *top > 0 && len(rows) > *top {
			rows = rows[:*top]
		}

		fmt.Printf("  %-9s %-9s %-7s %-6s %s\n", "SUGGEST", "STABLE", "ASKED", "VALUES", "QUERY")
		for _, s := range rows {
			fmt.Printf("  %-9s %-9s %-7d %-6d %s\n",
				round(s.Suggested), round(s.Stable), s.Executions, s.Params, trim(s.Shape, 70))
		}
		fmt.Println("\n  SUGGEST is what to cache it for, ten per cent inside STABLE, which is the")
		fmt.Println("  shortest stretch the same answer was observed to hold. ASKED counts this")
		fmt.Println("  query with these values; VALUES is how many different values the shape was")
		fmt.Println("  asked with, and a large one means there is little repetition to exploit.")
	}
	return nil
}

func round(d time.Duration) string {
	switch {
	case d >= time.Minute:
		return d.Round(time.Second).String()
	case d >= time.Second:
		return d.Round(100 * time.Millisecond).String()
	default:
		return d.Round(time.Millisecond).String()
	}
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
