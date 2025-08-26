-- Drop table if exists (be careful with this in production!)
DROP TABLE IF EXISTS hecr;

-- Create new table with same structure as users
CREATE TABLE hecr AS 
SELECT * FROM users WHERE 1=0;  -- This copies structure but no data

-- Add additional tracking columns if desired
ALTER TABLE hecr ADD COLUMN IF NOT EXISTS identified_via TEXT;
ALTER TABLE hecr ADD COLUMN IF NOT EXISTS keywords_matched TEXT[];
ALTER TABLE hecr ADD COLUMN IF NOT EXISTS date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Insert users who match health equity OR climate health keywords
INSERT INTO hecr
SELECT DISTINCT ON (u.id)
    u.*,
    'initial_scan' as identified_via,
    NULL as keywords_matched,
    CURRENT_TIMESTAMP as date_added
FROM users u
WHERE EXISTS (
    -- Check publications for any keyword match
    SELECT 1 FROM publications p
    WHERE p.user_id = u.id
    AND (
        LOWER(p.title) LIKE ANY(ARRAY[
            -- Health Equity keywords
            '%health equity%',
            '%health disparit%',
            '%healthcare access%',
            '%healthcare inequ%',
            '%racial disparit%',
            '%ethnic disparit%',
            '%minority health%',
            '%underserved population%',
            '%vulnerable population%',
            '%social determinant%',
            '%health inequalit%',
            '%socioeconomic disparit%',
            '%rural health%',
            '%urban health%',
            '%low-income health%',
            -- Climate Health keywords
            '%climate change%health%',
            '%climate medicine%',
            '%climate health%',
            '%planetary health%',
            '%environmental health%',
            '%heat wave%',
            '%heat stress%',
            '%thermal stress%',
            '%extreme weather%health%',
            '%air pollution%health%',
            '%vector-borne disease%',
            '%disease migration%',
            '%climate-sensitive disease%',
            '%zoonotic disease%',
            '%climate impact%health%'
        ])
    )
)
OR EXISTS (
    -- Check grants for any keyword match
    SELECT 1 FROM grants g
    WHERE g.user_id = u.id
    AND (
        LOWER(g.title) LIKE ANY(ARRAY[
            -- Same keywords as above
            '%health equity%',
            '%health disparit%',
            '%healthcare access%',
            '%healthcare inequ%',
            '%racial disparit%',
            '%ethnic disparit%',
            '%minority health%',
            '%underserved population%',
            '%vulnerable population%',
            '%social determinant%',
            '%health inequalit%',
            '%socioeconomic disparit%',
            '%rural health%',
            '%urban health%',
            '%low-income health%',
            '%climate change%health%',
            '%climate medicine%',
            '%climate health%',
            '%planetary health%',
            '%environmental health%',
            '%heat wave%',
            '%heat stress%',
            '%thermal stress%',
            '%extreme weather%health%',
            '%air pollution%health%',
            '%vector-borne disease%',
            '%disease migration%',
            '%climate-sensitive disease%',
            '%zoonotic disease%',
            '%climate impact%health%'
        ])
    )
)
ON CONFLICT (id) DO NOTHING;  -- Prevents duplicates if id is a primary key

-- Verify the results
SELECT COUNT(*) as users_found FROM hecr;

-- Optional: See which titles matched for verification
SELECT 
    h.id,
    h.firstname,
    h.lastname,
    COUNT(DISTINCT p.id) as matching_publications,
    COUNT(DISTINCT g.id) as matching_grants
FROM hecr h
LEFT JOIN publications p ON h.id = p.user_id
LEFT JOIN grants g ON h.id = g.user_id
WHERE 
    LOWER(p.title) LIKE ANY(ARRAY['%health equity%', '%climate%', '%disparit%', '%pollution%'])
    OR LOWER(g.title) LIKE ANY(ARRAY['%health equity%', '%climate%', '%disparit%', '%pollution%'])
GROUP BY h.id, h.firstname, h.lastname
ORDER BY matching_publications + matching_grants DESC;