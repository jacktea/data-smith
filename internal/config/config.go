package config

import (
	"encoding/json"
	"os"

	"github.com/jacktea/data-smith/pkg/config"
	"gopkg.in/yaml.v2"
)

// LoadConfig loads the application configuration from a YAML file.
func LoadConfig(path string) (*config.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg config.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// LoadRules loads the comparison rules from a JSON file.
func LoadRules(path string) (*config.RuleSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var ruleSet config.RuleSet
	if err := json.Unmarshal(data, &ruleSet); err != nil {
		return nil, err
	}

	return &ruleSet, nil
}
