def atualiza_area_construida_unidade(cur, reduzido):
    cur.execute(
        'SELECT id FROM dado_novo.unidade_imobiliaria WHERE lote_id = %s', (reduzido,))
    idUnidades = cur.fetchall()

    for id_tuple in idUnidades:
        id = id_tuple[0]  # Extrair o ID do tuple
        cur.execute(
            'SELECT st_area(ac.geom) FROM dado_novo.unidade_cobertura uc, dado_novo.area_coberta ac WHERE uc.unidade_id = %s AND uc.area_coberta_id = ac.id', (id,))
        areas = cur.fetchall()
        total_area = sum(area[0] for area in areas if area[0] is not None)

        # atualiza a tabela unidade_imobiliaria com a soma das áreas
        cur.execute(
            'UPDATE dado_novo.unidade_imobiliaria SET area_construcao = %s WHERE id = %s', (total_area, id,))
