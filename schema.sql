CREATE TABLE IF NOT EXISTS user_agents (
  id INT AUTO_INCREMENT PRIMARY KEY,
  agent TEXT NOT NULL,
  UNIQUE KEY (agent(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE TABLE IF NOT EXISTS scraping_targets (
  id INT AUTO_INCREMENT PRIMARY KEY,
  url TEXT NOT NULL,
  title TEXT NOT NULL,
  owner TEXT NOT NULL,
  ownerurl TEXT NOT NULL,
  check_lastmodified BOOLEAN NOT NULL,
  tag TEXT,
  tag_id TEXT,
  tag_class TEXT,
  email_recipient TEXT,
  qmd_name CHAR(8) NOT NULL,
  UNIQUE KEY (url(255)),
  UNIQUE KEY (qmd_name),
  CHECK (check_lastmodified = TRUE OR (check_lastmodified = FALSE AND tag IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE TABLE IF NOT EXISTS scraping_results (
  id INT AUTO_INCREMENT PRIMARY KEY,
  target_id INT NOT NULL,
  last_content TEXT,
  last_update DATETIME,
  content_hash CHAR(255),
  FOREIGN KEY (target_id) REFERENCES scraping_targets(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE TABLE IF NOT EXISTS archive_urls (
  id INT AUTO_INCREMENT PRIMARY KEY,
  target_id INT NOT NULL,
  archive_url TEXT NOT NULL,
  created_at DATETIME NOT NULL,
  FOREIGN KEY (target_id) REFERENCES scraping_targets(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE TABLE IF NOT EXISTS action_logs (
  id INT AUTO_INCREMENT PRIMARY KEY,
  target_id INT NOT NULL,
  action_type ENUM('check', 'archive', 'tweet', 'toot', 'email') NOT NULL,
  status BOOLEAN NOT NULL DEFAULT FALSE,
  action_time DATETIME NOT NULL,
  message TEXT,
  FOREIGN KEY (target_id) REFERENCES scraping_targets(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE TABLE IF NOT EXISTS email_settings (
  id INT AUTO_INCREMENT PRIMARY KEY,
  smtp_from VARCHAR(255) NOT NULL,
  smtp_to VARCHAR(255) NOT NULL,
  subject_template TEXT NOT NULL,
  body_template TEXT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE INDEX idx_scraping_targets_url ON scraping_targets (url(255));
CREATE INDEX idx_scraping_results_target_id ON scraping_results (target_id);
CREATE INDEX idx_scraping_results_last_update ON scraping_results (last_update);
CREATE INDEX idx_archive_urls_target_id ON archive_urls (target_id);
CREATE INDEX idx_action_logs_target_id ON action_logs (target_id);
CREATE INDEX idx_action_logs_action_time ON action_logs (action_time);

CREATE OR REPLACE VIEW qmd_view AS
SELECT DISTINCT
    t.owner,
    t.title,
    t.qmd_name
FROM 
    scraping_targets t
LEFT JOIN 
    scraping_results r ON t.id = r.target_id
WHERE 
    t.check_lastmodified = 1 OR
    (t.check_lastmodified = 0 AND 
     (SELECT COUNT(DISTINCT last_update) FROM scraping_results WHERE target_id = t.id) > 1)
ORDER BY 
    t.owner, t.title;

CREATE OR REPLACE VIEW updated_targets_view AS
SELECT 
    t.id,
    r.last_update,
    t.owner,
    t.ownerurl,
    t.title,
    t.url,
    a.archive_url,
    t.qmd_name
FROM 
    scraping_targets t
JOIN 
    scraping_results r ON t.id = r.target_id
LEFT JOIN (
    SELECT target_id, archive_url
    FROM (
        SELECT target_id, archive_url,
               ROW_NUMBER() OVER (PARTITION BY target_id ORDER BY id DESC) AS row_num
        FROM archive_urls
    ) a
    WHERE row_num = 1
) a ON t.id = a.target_id
WHERE 
    (t.check_lastmodified = TRUE) OR
    (t.check_lastmodified = FALSE AND r.id NOT IN (
        SELECT id
        FROM (
            SELECT id,
                   ROW_NUMBER() OVER (PARTITION BY target_id ORDER BY last_update ASC) AS rn_asc,
                   ROW_NUMBER() OVER (PARTITION BY target_id ORDER BY last_update DESC) AS rn_desc
            FROM scraping_results
        ) sub
        WHERE rn_asc = 1 OR rn_desc = 1
    ))
ORDER BY 
    r.last_update DESC;
