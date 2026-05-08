-- Comparação entre décadas e evolução ano a ano.
-- Responde: a banda ficou fazendo músicas mais longas ou mais curtas com o tempo, qual decada teve mais músicas
-- gravadas?, qual ano foi o mais produtivo?

SELECT
    (FLOOR(EXTRACT(YEAR FROM a."data_lançamento"::DATE) / 10) * 10)
        ::INTEGER || 's'                                            AS decada,
    EXTRACT(YEAR FROM a."data_lançamento"::DATE)::INTEGER           AS ano,

    COUNT(DISTINCT a.id_album)                                      AS total_albums,
    COUNT(t.id_musica)                                              AS total_musicas,
    ROUND(AVG(t.duracao_ms) / 60000.0, 2)                           AS media_duracao_min,
    ROUND(MIN(t.duracao_ms) / 60000.0, 2)                           AS menor_duracao_min,
    ROUND(MAX(t.duracao_ms) / 60000.0, 2)                           AS maior_duracao_min,
    ROUND(SUM(t.duracao_ms) / 60000.0, 1)                           AS total_minutos_gravados

FROM silver_albums  a
JOIN silver_tracks  t ON t.id_album = a.id_album
GROUP BY decada, ano
ORDER BY ano;