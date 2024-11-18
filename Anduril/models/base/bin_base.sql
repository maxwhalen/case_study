SELECT
    id AS bin_id,
    name AS bin
FROM {{ source('Anduril', 'bin') }}
