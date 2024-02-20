
def carregar_benfeitoria(reduzido, cur):
    try:
        # Busca por um array de coberturas que estejam dentro do lote
        cur.execute("SELECT array_agg(b.geom) FROM dado_novo.lote l, public.benfeitoria b "
                    "WHERE ST_CONTAINS(l.geom, b.geom) AND l.id = %s", (reduzido,))
        benfeitorias_temp = cur.fetchone()

        # Busca todas as unidades pertencentes ao lote
        cur.execute(
            "SELECT id FROM dado_novo.unidade_imobiliaria WHERE lote_id = %s", (reduzido,))
        unidades = cur.fetchall()

        # Cadastra todas as cobertura
        for benfeitoria_geom in benfeitorias_temp:
            cur.execute(
                'INSERT INTO dado_novo.benfeitoria (geom) VALUES (%s) RETURNING id', (benfeitoria_geom,))
            benfeitoria_id = cur.fetchone()

            # Associando elas as unidades
            for unidade in unidades:
                unidade_id = unidade['id']
                cur.execute("INSERT INTO dado_novo.unidade_benfeitoria (unidade_id, benfeitoria_id) "
                            "VALUES (%s, %s)", (unidade_id, benfeitoria_id))

        return True

    except Exception as e:
        print('Erro ao criar e associar cobertura:', e)
        return False
