
def carregar_cobertura(reduzido, cur):
    try:
        # Busca por um array de coberturas que estejam dentro do lote
        cur.execute("SELECT array_agg(ac.geom) FROM dado_novo.lote l, public.area_coberta ac "
                    "WHERE ST_CONTAINS(l.geom, ac.geom) AND l.id = %s", (reduzido,))
        coberturas_temp = cur.fetchone()

        # Busca todas as unidades pertencentes ao lote
        cur.execute(
            "SELECT id FROM dado_novo.unidade_imobiliaria WHERE lote_id = %s", (reduzido,))
        unidades = cur.fetchall()

        # Cadastra todas as cobertura
        for cobertura_geom in coberturas_temp:
            cur.execute(
                "INSERT INTO dado_novo.area_coberta (geom) VALUES (%s) RETURNING id", (cobertura_geom,))
            cobertura_id = cur.fetchone()

            # Associando elas as unidades
            for unidade in unidades:
                unidade_id = unidade['id']
                cur.execute("INSERT INTO dado_novo.unidade_cobertura (unidade_id, area_coberta_id) "
                            "VALUES (%s, %s)", (unidade_id, cobertura_id))

        return True

    except Exception as e:
        print('Erro ao criar e associar cobertura:', e)
        return False
