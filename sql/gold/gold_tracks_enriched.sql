-- Visão enriquecida das músicas com contexto do álbum, rankings e classificação.

SELECT
    t.nome_musica,
    t.numero_no_album,
    t.duracao_min_seg,

    -- Classificação por faixa de duração
    CASE
        WHEN t.duracao_ms <  90000  THEN 'curta (< 1:30)'
        WHEN t.duracao_ms < 180000  THEN 'média (1:30 - 3:00)'
        WHEN t.duracao_ms < 300000  THEN 'padrão (3:00 - 5:00)'
        WHEN t.duracao_ms < 480000  THEN 'longa (5:00 - 8:00)'
        ELSE                             'muito longa (> 8:00)'
    END                                                             AS faixas_classificacao,

    -- Posição no álbum
    CASE
        WHEN t.numero_no_album = 1                                  THEN 'abertura'
        WHEN t.numero_no_album = a.qtd_faixas                       THEN 'encerramento'
        WHEN t.numero_no_album <= CEIL(a.qtd_faixas / 3.0)          THEN 'início'
        WHEN t.numero_no_album <= CEIL(a.qtd_faixas * 2.0 / 3.0)    THEN 'meio'
        ELSE                                                          'fim'
    END                                                             AS posicao_no_album,

    -- Ranking de duração dentro do próprio álbum (1 = mais longa)
    RANK() OVER (
        PARTITION BY t.id_album
        ORDER BY t.duracao_ms DESC
    )                                                               AS rank_duracao_no_album,

    -- Ranking global entre todas as músicas
    RANK() OVER (ORDER BY t.duracao_ms DESC)                        AS rank_duracao_global,

    t."Reproduzivel"                                                AS reproduzivel,

    -- Conteudo explicito
    CASE
        WHEN t.conteudo_explicito = False                           THEN 'nenhum'
        WHEN t.conteudo_explicito = True                            THEN 'contém'
    END                                                             AS conteudo_explicito,

    -- Contexto do álbum 
    a.nome_album,
    t.id_musica,
    a.id_album,
    t.url_da_musica,
    a.capa_album_300,
    NOW()::TEXT                                                     AS loaded_at

FROM silver_tracks  t
JOIN silver_albums  a ON a.id_album = t.id_album
ORDER BY a."data_lançamento", t.numero_no_album;