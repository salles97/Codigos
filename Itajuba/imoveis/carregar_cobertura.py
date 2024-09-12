def carregar_cobertura(reduzido, cur, arquivo_log):
    try:
        # Busca por um array de coberturas que estejam dentro do lote
        cur.execute("SELECT array_agg(ST_AsText(ac.geom)) FROM dado_novo.lote l, public.area_coberta ac "
                    "WHERE ST_CONTAINS(l.geom, ac.geom) AND l.id = %s", (reduzido,))
        coberturas_temp = cur.fetchone()[0]

        # Busca todas as unidades pertencentes ao lote
        cur.execute(
            "SELECT id FROM dado_novo.unidade_imobiliaria WHERE lote_id = %s", (reduzido,))
        unidades = cur.fetchall()

        # Cadastra todas as cobertura
        for cobertura_geom in coberturas_temp:
            cur.execute(
                "INSERT INTO dado_novo.area_coberta (geom) VALUES (ST_GeomFromText(%s)) RETURNING id", (cobertura_geom,))
            cobertura_id = cur.fetchone()[0]

            # Associando elas as unidades
            for unidade in unidades:
                unidade_id = unidade['id']
                cur.execute("INSERT INTO dado_novo.unidade_cobertura (unidade_id, area_coberta_id) "
                            "VALUES (%s, %s)", (unidade_id, cobertura_id))

        return True

    except Exception as e:
        arquivo_log.write(f'Erro ao criar e associar cobertura para o lote {reduzido}: {e}\n')
        return False
