-- ============================================================
-- Cattle Scraper Production — Supabase Schema
-- Run this in the Supabase SQL Editor to set up all tables
-- ============================================================

-- 1. Contacts table (main email list)
CREATE TABLE IF NOT EXISTS contacts (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    farm_name TEXT DEFAULT '',
    owner_name TEXT DEFAULT '',
    phone TEXT DEFAULT '',
    address TEXT DEFAULT '',
    city TEXT DEFAULT '',
    state TEXT DEFAULT '',
    zip_code TEXT DEFAULT '',
    website TEXT DEFAULT '',
    facebook TEXT DEFAULT '',
    instagram TEXT DEFAULT '',
    cattle_type TEXT DEFAULT '',
    breed TEXT DEFAULT '',
    head_count TEXT DEFAULT '',
    source_url TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_contacts_state ON contacts(state);
CREATE INDEX IF NOT EXISTS idx_contacts_cattle_type ON contacts(cattle_type);
CREATE INDEX IF NOT EXISTS idx_contacts_created ON contacts(created_at);

-- 2. URL queue + tracking
CREATE TABLE IF NOT EXISTS urls (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    status TEXT DEFAULT 'pending',        -- pending | processing | completed | failed
    source TEXT DEFAULT '',               -- search | directory | association
    discovered_by TEXT DEFAULT '',        -- query string or crawler name
    state_target TEXT DEFAULT '',         -- which state search this came from
    emails_found INT DEFAULT 0,
    error TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_urls_status ON urls(status);
CREATE INDEX IF NOT EXISTS idx_urls_state ON urls(state_target);

-- 3. Scrape jobs (job tracking)
CREATE TABLE IF NOT EXISTS scrape_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,               -- search | directories | associations | full
    states TEXT[] NOT NULL,               -- array of state names
    status TEXT DEFAULT 'queued',         -- queued | running | completed | failed
    query_index INT DEFAULT 0,           -- current position in query list
    total_queries INT DEFAULT 0,
    urls_discovered INT DEFAULT 0,
    urls_processed INT DEFAULT 0,
    emails_found INT DEFAULT 0,
    error TEXT DEFAULT '',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. Search queries (track completed queries)
CREATE TABLE IF NOT EXISTS search_queries (
    id BIGSERIAL PRIMARY KEY,
    query TEXT NOT NULL UNIQUE,
    results_count INT DEFAULT 0,
    urls_found INT DEFAULT 0,
    job_id BIGINT REFERENCES scrape_jobs(id) ON DELETE SET NULL,
    executed_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. Auto-update updated_at on contacts
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS contacts_updated_at ON contacts;
CREATE TRIGGER contacts_updated_at
    BEFORE UPDATE ON contacts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- 6. Helper function: get dashboard stats
CREATE OR REPLACE FUNCTION get_dashboard_stats()
RETURNS JSON AS $$
DECLARE
    result JSON;
BEGIN
    SELECT json_build_object(
        'total_emails', (SELECT COUNT(*) FROM contacts),
        'total_urls', (SELECT COUNT(*) FROM urls),
        'urls_pending', (SELECT COUNT(*) FROM urls WHERE status = 'pending'),
        'urls_completed', (SELECT COUNT(*) FROM urls WHERE status = 'completed'),
        'urls_failed', (SELECT COUNT(*) FROM urls WHERE status = 'failed'),
        'active_jobs', (SELECT COUNT(*) FROM scrape_jobs WHERE status = 'running'),
        'completed_jobs', (SELECT COUNT(*) FROM scrape_jobs WHERE status = 'completed'),
        'emails_today', (SELECT COUNT(*) FROM contacts WHERE created_at >= CURRENT_DATE),
        'emails_this_week', (SELECT COUNT(*) FROM contacts WHERE created_at >= CURRENT_DATE - INTERVAL '7 days')
    ) INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- 7. Helper function: emails per state
CREATE OR REPLACE FUNCTION get_emails_per_state()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT state, COUNT(*) as count
            FROM contacts
            WHERE state != ''
            GROUP BY state
            ORDER BY count DESC
        ) t
    );
END;
$$ LANGUAGE plpgsql;

-- 8. Helper function: emails per hour (last 24h)
CREATE OR REPLACE FUNCTION get_emails_per_hour()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT
                date_trunc('hour', created_at) as hour,
                COUNT(*) as count
            FROM contacts
            WHERE created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY date_trunc('hour', created_at)
            ORDER BY hour
        ) t
    );
END;
$$ LANGUAGE plpgsql;

-- 9. Helper function: performance metrics (single query replaces 16 individual queries)
CREATE OR REPLACE FUNCTION get_performance_metrics()
RETURNS JSON AS $$
DECLARE
    result JSON;
BEGIN
    SELECT json_build_object(
        'last_1min',  (SELECT COUNT(*) FROM contacts WHERE created_at >= NOW() - INTERVAL '1 minute'),
        'last_5min',  (SELECT COUNT(*) FROM contacts WHERE created_at >= NOW() - INTERVAL '5 minutes'),
        'last_15min', (SELECT COUNT(*) FROM contacts WHERE created_at >= NOW() - INTERVAL '15 minutes'),
        'last_1hr',   (SELECT COUNT(*) FROM contacts WHERE created_at >= NOW() - INTERVAL '1 hour'),
        'last_24hr',  (SELECT COUNT(*) FROM contacts WHERE created_at >= NOW() - INTERVAL '24 hours'),
        'urls_last_1min',  (SELECT COUNT(*) FROM urls WHERE created_at >= NOW() - INTERVAL '1 minute'),
        'urls_last_5min',  (SELECT COUNT(*) FROM urls WHERE created_at >= NOW() - INTERVAL '5 minutes'),
        'urls_last_15min', (SELECT COUNT(*) FROM urls WHERE created_at >= NOW() - INTERVAL '15 minutes'),
        'urls_last_1hr',   (SELECT COUNT(*) FROM urls WHERE created_at >= NOW() - INTERVAL '1 hour'),
        'urls_last_24hr',  (SELECT COUNT(*) FROM urls WHERE created_at >= NOW() - INTERVAL '24 hours'),
        'total_emails',   (SELECT COUNT(*) FROM contacts),
        'total_urls',     (SELECT COUNT(*) FROM urls),
        'urls_completed', (SELECT COUNT(*) FROM urls WHERE status = 'completed'),
        'urls_pending',   (SELECT COUNT(*) FROM urls WHERE status = 'pending'),
        'per_minute',     ROUND((SELECT COUNT(*) FROM contacts WHERE created_at >= NOW() - INTERVAL '5 minutes')::numeric / 5, 1),
        'per_hour',       (SELECT COUNT(*) FROM contacts WHERE created_at >= NOW() - INTERVAL '1 hour'),
        'urls_per_minute', ROUND((SELECT COUNT(*) FROM urls WHERE created_at >= NOW() - INTERVAL '5 minutes')::numeric / 5, 1),
        'urls_per_hour',  (SELECT COUNT(*) FROM urls WHERE created_at >= NOW() - INTERVAL '1 hour'),
        'scraper_start',  (SELECT started_at FROM scrape_jobs WHERE status = 'running' ORDER BY started_at LIMIT 1),
        'first_contact',  (SELECT created_at FROM contacts ORDER BY created_at LIMIT 1)
    ) INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- 10. Helper function: emails by country and state
CREATE OR REPLACE FUNCTION get_emails_by_country_state()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT country, state, COUNT(*) as cnt
            FROM contacts
            WHERE country IS NOT NULL AND country != ''
            GROUP BY country, state
            ORDER BY country, cnt DESC
        ) t
    );
END;
$$ LANGUAGE plpgsql;

-- 11. Indexes for time-windowed queries (speeds up performance metrics)
CREATE INDEX IF NOT EXISTS idx_contacts_created_at ON contacts(created_at);
CREATE INDEX IF NOT EXISTS idx_urls_created_at ON urls(created_at);

-- 12. Add country column to urls if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'urls' AND column_name = 'country') THEN
        ALTER TABLE urls ADD COLUMN country TEXT DEFAULT 'US';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'contacts' AND column_name = 'country') THEN
        ALTER TABLE contacts ADD COLUMN country TEXT DEFAULT 'US';
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_contacts_country ON contacts(country);
CREATE INDEX IF NOT EXISTS idx_urls_country ON urls(country);
