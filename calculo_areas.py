def atualiza_area_construida_unidade(cur, reduzido):
    cur.execute(
        'SELECT id FROM dado_novo.unidade_imobiliaria WHERE lote_id = %s', (reduzido,))
    idUnidades = cur.fetchall()

    for id in idUnidades:
        cur.execute(
            'SELECT st_area(geom) FROM dado_novo.area_coberta WHERE unidade_id = %s', (id,))
        areas = cur.fetchall()
        total_area = sum(area[0] for area in areas if area[0]
                         is not None)  # soma as áreas

        # atualiza a tabela unidade_imobiliaria com a soma das áreas
        cur.execute(
            'UPDATE dado_novo.unidade_imobiliaria SET area_construida = %s WHERE id = %s', (total_area, id,))
