package store

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Conn represents a single WhatsApp connection owned by a tenant.
type Conn struct {
	ConnectionID string
	TenantID     string
	JID          string
	WebhookURL   string
	Token        string
}

// Store wraps the whatsmeow device container plus our own connections table.
type Store struct {
	Container *sqlstore.Container
	DB        *sql.DB
}

const createConnectionsTable = `
CREATE TABLE IF NOT EXISTS connections (
	connection_id TEXT PRIMARY KEY,
	tenant_id     TEXT NOT NULL,
	jid           TEXT,
	webhook_url   TEXT,
	token         TEXT NOT NULL,
	created_at    TIMESTAMPTZ DEFAULT now()
)`

// Open connects to Postgres, initializes the whatsmeow sqlstore container and
// ensures the connections table exists.
func Open(ctx context.Context, dsn string) (*Store, error) {
	logger := waLog.Stdout("Store", "INFO", true)

	container, err := sqlstore.New(ctx, "postgres", dsn, logger)
	if err != nil {
		return nil, fmt.Errorf("open sqlstore container: %w", err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sql db: %w", err)
	}

	if _, err := db.ExecContext(ctx, createConnectionsTable); err != nil {
		return nil, fmt.Errorf("create connections table: %w", err)
	}

	return &Store{Container: container, DB: db}, nil
}

// UpsertConn inserts or updates a connection row by connection_id.
func (s *Store) UpsertConn(ctx context.Context, c Conn) error {
	const q = `
INSERT INTO connections (connection_id, tenant_id, jid, webhook_url, token)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (connection_id) DO UPDATE SET
	tenant_id   = EXCLUDED.tenant_id,
	jid         = EXCLUDED.jid,
	webhook_url = EXCLUDED.webhook_url,
	token       = EXCLUDED.token`
	if _, err := s.DB.ExecContext(ctx, q, c.ConnectionID, c.TenantID, c.JID, c.WebhookURL, c.Token); err != nil {
		return fmt.Errorf("upsert conn: %w", err)
	}
	return nil
}

// ListConns returns all stored connections.
func (s *Store) ListConns(ctx context.Context) ([]Conn, error) {
	const q = `SELECT connection_id, tenant_id, jid, webhook_url, token FROM connections ORDER BY created_at`
	rows, err := s.DB.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list conns: %w", err)
	}
	defer rows.Close()

	var conns []Conn
	for rows.Next() {
		var c Conn
		var jid, webhookURL sql.NullString
		if err := rows.Scan(&c.ConnectionID, &c.TenantID, &jid, &webhookURL, &c.Token); err != nil {
			return nil, fmt.Errorf("scan conn: %w", err)
		}
		c.JID = jid.String
		c.WebhookURL = webhookURL.String
		conns = append(conns, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate conns: %w", err)
	}
	return conns, nil
}
