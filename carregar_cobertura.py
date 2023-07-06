
def carregar_cobertura(reduzido, cur):
    try:
        reduzido_str = str(reduzido)

        cur.execute("SELECT array_agg(ac.geom) FROM public.lote_ajuste l, public.area_coberta_ajuste ac "
                    "WHERE ST_CONTAINS(l.geom, ac.geom) AND l.name = %s", (reduzido_str,))
        coberturas_temp = cur.fetchone()[0]

        for cobertura_geom in coberturas_temp:
            cur.execute(
                "INSERT INTO dado_novo.area_coberta (geom) VALUES (%s) RETURNING id", (cobertura_geom,))
            cobertura_id = cur.fetchone()[0]

            cur.execute(
                "SELECT id FROM dado_novo.unidade_imobiliaria WHERE lote_id = %s", (reduzido,))
            unidades = cur.fetchall()

            for unidade in unidades:
                unidade_id = unidade['id']
                cur.execute("INSERT INTO dado_novo.unidade_cobertura (unidade_id, area_coberta_id) "
                            "VALUES (%s, %s)", (unidade_id, cobertura_id))

        return True

    except Exception as e:
        print('Erro ao criar e associar cobertura:', e)
        return False
