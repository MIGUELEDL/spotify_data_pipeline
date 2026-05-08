-- tabela de métricas e rankings, junção de albums e músicas, músicas com maior e menor duração, média de durações
-- durações de músicas de cada álbum, ranking de albums com maior número de faixas, duração total, média 

WITH base AS (
    SELECT
        a.id_album,
        a.nome_album,
        a."data_lançamento",
        COUNT(t.id_musica)                                           AS total_musicas,
        SUM(t.duracao_ms)                                            AS duracao_total_ms,
        ROUND(AVG(t.duracao_ms) / 60000.0, 2)                        AS media_duracao_tracks,
        a.capa_album_300
    FROM silver_albums a
    LEFT JOIN silver_tracks t ON t.id_album = a.id_album
    GROUP BY a.id_album, a.nome_album, a."data_lançamento", a.capa_album_300
),
musica_mais_curta AS (
    SELECT DISTINCT ON (id_album)
        id_album,
        nome_musica     AS musica_mais_curta,
        duracao_min_seg AS duracao_mais_curta
    FROM silver_tracks
    ORDER BY id_album, duracao_ms ASC
),
musica_mais_longa AS (
    SELECT DISTINCT ON (id_album)
        id_album,
        nome_musica     AS musica_mais_longa,
        duracao_min_seg AS duracao_mais_longa
    FROM silver_tracks
    ORDER BY id_album, duracao_ms DESC
)

SELECT
    b.nome_album,
    b.total_musicas,

    -- Durações
    -- Duração total formatada
    printf('%02d:%02d:%02d',
        (b.duracao_total_ms::BIGINT // 3600000),
        ((b.duracao_total_ms::BIGINT % 3600000) // 60000),
        ((b.duracao_total_ms::BIGINT % 60000) // 1000)
    )                                                                AS duracao_total_hms,
    ROUND(b.duracao_total_ms / 60000.0, 1)                           AS duracao_total_min,
    b.media_duracao_tracks,

    -- Extremos do álbum
    ml.musica_mais_longa,
    ml.duracao_mais_longa,
    mc.musica_mais_curta,
    mc.duracao_mais_curta,

    -- Rankings
    RANK() OVER (ORDER BY b.total_musicas DESC)                      AS rank_mais_faixas,
    RANK() OVER (ORDER BY b.duracao_total_ms DESC)                   AS rank_duracao_total,
    RANK() OVER (ORDER BY b.media_duracao_tracks DESC)               AS rank_media_duracao,

    b.id_album,
    b.capa_album_300,
    NOW()::TEXT                                                      AS loaded_at

FROM base              b
LEFT JOIN musica_mais_longa ml ON ml.id_album = b.id_album
LEFT JOIN musica_mais_curta mc ON mc.id_album = b.id_album
ORDER BY b."data_lançamento";