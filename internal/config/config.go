package config

import "os"

type Config struct {
	Port       string
	PGDSN      string
	AdminToken string
}

func Load() Config {
	return Config{
		Port:       getenv("GW_PORT", "3020"),
		PGDSN:      os.Getenv("GW_PG_DSN"),
		AdminToken: os.Getenv("GW_ADMIN_TOKEN"),
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
