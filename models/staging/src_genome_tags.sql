WITH raw_genome_tags AS (
  SELECT * FROM {{ source('raw', 'raw_genome_tags') }}
)

SELECT
  tagId AS tag_id,
  tag
FROM raw_genome_tags