package config

import (
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

// The short form of cline.yaml: two URIs, the router's address, and one line
// per collection.
//
//	hot:  mongodb://fast1,fast2,fast3/?replicaSet=rsFast
//	cold: mongodb://old1,old2,old3/?replicaSet=rsOld
//	listen: 0.0.0.0:27017
//	collections:
//	  app.orders: 90d
//	  app.events: {older_than: 30d, date_field: createdAt}
//
// The long form wrote one decision three times. A tiered collection was named
// in feeder.namespaces, again in evictor.rules with the age past which it
// leaves the fast tier, and again in warmer.rules with how far back to fill
// it, which is the same age. A file that changed one and not the others
// evicted what it then warmed back, or mirrored a collection nothing evicted.
// collections says it once and Load writes the three.
//
// Every long form key still works, for the settings the short form leaves at
// their defaults. Naming one collection both ways is refused rather than
// merged, because which of two ages was meant is not something to guess.

// Collection is one tiered collection in the short form.
type Collection struct {
	// OlderThan is the age past which a document leaves the fast tier, and
	// so also how far back the warmer fills it.
	OlderThan Duration
	// DateField holds the document's age. Empty uses the timestamp in a
	// default ObjectId _id.
	DateField string
}

// UnmarshalYAML takes "90d", or a mapping with older_than and date_field.
func (c *Collection) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		return node.Decode(&c.OlderThan)
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: a collection takes an age such as 90d, or older_than and date_field", node.Line)
	}
	if err := knownKeys(node, "older_than", "date_field"); err != nil {
		return err
	}
	var long struct {
		OlderThan Duration `yaml:"older_than"`
		DateField string   `yaml:"date_field"`
	}
	if err := node.Decode(&long); err != nil {
		return err
	}
	c.OlderThan, c.DateField = long.OlderThan, long.DateField
	return nil
}

// UnmarshalYAML takes a connection string, or the long form's mapping with uri.
func (t *Tier) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		return node.Decode(&t.URI)
	}
	if err := knownKeys(node, "uri"); err != nil {
		return err
	}
	var long struct {
		URI string `yaml:"uri"`
	}
	if err := node.Decode(&long); err != nil {
		return err
	}
	t.URI = long.URI
	return nil
}

// knownKeys refuses a mapping key outside names. A custom unmarshaller is
// decoded without the strictness Load asks of the rest of the file, so a
// misspelt date_field would otherwise be a silence: the rule would age the
// collection by _id and nothing would say so.
func knownKeys(node *yaml.Node, names ...string) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: expected a mapping", node.Line)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		k := node.Content[i]
		ok := false
		for _, n := range names {
			ok = ok || k.Value == n
		}
		if !ok {
			return fmt.Errorf("line %d: field %s not found", k.Line, k.Value)
		}
	}
	return nil
}

// expandShortForm writes listen and collections into the long form's places.
// raw is the file, read again to tell a router.listen somebody wrote from the
// default.
func (c *Config) expandShortForm(raw []byte) error {
	if c.Listen != "" {
		var written struct {
			Router struct {
				Listen *string `yaml:"listen"`
			} `yaml:"router"`
		}
		if err := yaml.Unmarshal(raw, &written); err == nil && written.Router.Listen != nil {
			return fmt.Errorf("listen and router.listen are both set (%q and %q): keep one",
				c.Listen, *written.Router.Listen)
		}
		c.Router.Listen = c.Listen
	}
	if len(c.Collections) == 0 {
		return nil
	}
	written := map[string]string{}
	for _, ns := range c.Feeder.Namespaces {
		written[ns] = "feeder.namespaces"
	}
	for _, r := range c.Evictor.Rules {
		written[r.NS] = "evictor.rules"
	}
	for _, r := range c.Warmer.Rules {
		written[r.NS] = "warmer.rules"
	}
	names := make([]string, 0, len(c.Collections))
	for ns := range c.Collections {
		names = append(names, ns)
	}
	sort.Strings(names)
	for _, ns := range names {
		if where, ok := written[ns]; ok {
			return fmt.Errorf("%s is in collections and in %s: name it in one place", ns, where)
		}
		col := c.Collections[ns]
		if col.OlderThan <= 0 {
			return fmt.Errorf("collections: %s needs an age, such as %s: 90d", ns, ns)
		}
		c.Feeder.Namespaces = append(c.Feeder.Namespaces, ns)
		c.Evictor.Rules = append(c.Evictor.Rules,
			EvictionRule{NS: ns, OlderThan: col.OlderThan, DateField: col.DateField})
		c.Warmer.Rules = append(c.Warmer.Rules,
			WarmRule{NS: ns, Back: col.OlderThan, DateField: col.DateField})
	}
	return nil
}
