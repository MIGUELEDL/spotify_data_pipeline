-- Tabela de albums enriquecida com algumas métricas como durações totais, medias, data e decada de cada albúm

SELECT
    a.id_album,
    a.nome_album,
    a."data_lançamento"::DATE                                        AS data_lancamento,
    (FLOOR(EXTRACT(YEAR FROM a."data_lançamento"::DATE) / 10) * 10)
        ::INTEGER || 's'                                             AS decada,

    -- Faixas
    COUNT(t.id_musica)                                               AS qtd_faixas,

    -- Duração total
    printf('%02d:%02d:%02d',
        (SUM(t.duracao_ms)::BIGINT // 3600000),
        ((SUM(t.duracao_ms)::BIGINT % 3600000) // 60000),
        ((SUM(t.duracao_ms)::BIGINT % 60000) // 1000)
    )                                                                AS duracao_total_hms,
    SUM(t.duracao_ms)                                                AS duracao_total_ms,
    ROUND(SUM(t.duracao_ms) / 60000.0, 2)                            AS duracao_total_min,

    -- Duração média por faixa
    ROUND(AVG(t.duracao_ms) / 60000.0, 2)                            AS media_duracao_tracks,

    -- Capas
    a.capa_album_640,
    a.capa_album_300,
    a.capa_album_64,

    NOW()::TEXT                                                      AS loaded_at

FROM silver_albums  a
LEFT JOIN silver_tracks t ON t.id_album = a.id_album
GROUP BY
    a.id_album, a.nome_album, a."data_lançamento",
    a.capa_album_640, a.capa_album_300, a.capa_album_64
ORDER BY a."data_lançamento";