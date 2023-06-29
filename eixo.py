def eixos(cur):
    # Seleciona todos os eixos do banco de dados.
    cur.execute('select geom , name, id from public.eixo')
    eixos = cur.fetchall()

    # Define uma variável booleana para verificar se algum eixo não tem um logradouro correspondente.
    nao_encontrado = 0

    # Para cada eixo, procura o nome do logradouro correspondente.
    for eixo in eixos:

        # Faz uma consulta SQL para encontrar o logradouro que tem a maior interseção com o eixo.
        cur.execute("""
            SELECT l.id, l.nome, ST_Length(ST_Intersection(l.geom, e.geom)) / ST_Length(l.geom) AS overlap
            FROM dado_novo.logradouro l, public.eixo e
            WHERE ST_Intersects(l.geom, e.geom) AND e.id = %s
            ORDER BY overlap DESC LIMIT 1
        """, (eixo[2],))
        logName = cur.fetchall()

        # Se um logradouro foi encontrado, insere o eixo com a referência ao logradouro.
        if logName:
            # Verifica se o eixo intersecciona com mais de um logradouro (indicativo de um erro de vetorização).
            if len(logName) > 1:
                print('Eixo intersecciona com mais de um logradouro. ')
                print((logName))

            # Insere o eixo com a referência ao logradouro encontrado.
            cur.execute("""
                INSERT INTO dado_novo.eixo (geom, logradouro_nome, logradouro_id) 
                VALUES (%s, %s, %s)
            """, (str(eixo[0]), str(logName[0][1]), str(logName[0][0])))
        # Se nenhum logradouro foi encontrado, insere o eixo sem referência a logradouro.
        else:
            nao_encontrado += 1
            # Se a variável for igual a 1, imprime a mensagem.
            if nao_encontrado == 1:
                print('Não foi encontrado nenhum logradouro para os eixos: ')

            print(eixo[1])
            cur.execute("""
                INSERT INTO dado_novo.eixo (geom, logradouro_nome, logradouro_id) 
                VALUES (%s, %s, NULL)
            """, (str(eixo[0]), str(eixo[1])))

    print("----------Fim eixos-----------------")
