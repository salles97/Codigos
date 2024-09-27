def enderecos(cur):
    # Solicitamos todas as entradas da tabela "lote" na base de dados da prefeitura
    cur.execute('select * from public.lote')
    recset = cur.fetchall()

    # Iteramos através de cada registro recebido
    for rec in recset:
        # rec[3] corresponde ao nome do lote, rotulagem informada pela equipe de vetorizacao
        name = rec[3]

        # Se o nome contiver algum dos caracteres '[', ']', '/' ou 'Property', apresenta adversidade identificada pela vetorizacao.
        if any(char in name for char in ['[', ']', '/', 'Property']):
            print(f"Lote sem info: {name}")
        else:
            # Se o nome contiver um '(' removemos tudo apos ele
            if '(' in name:
                name = name.split('(')[0]

            # Separamos o nome por '-' e verificamos a quantidade de partes e alguns valores específicos
            inscri = name.split('-')
            if len(inscri) == 3 or inscri[3] in ['AI', 'AV', 'APP', 'AR']:
                # Se a terceira parte do nome for 'AV', 'AI', 'APP', imprimimos uma mensagem específica
                if inscri[2] in ['AV', 'AI', 'APP']:
                    print('Área Verde ou Institucional')
                else:
                    # Usamos UNION para selecionar os registros únicos de duas tabelas, onde algumas das condicões de pesquisa correspondem ao nome
                    cur.execute("""
                                SELECT endereco_id FROM dado_antigo.unidade_imobiliaria WHERE setor_cod = %s AND quadra_cod = %s AND lote_cod = %s
                                UNION
                                SELECT id FROM dado_antigo.lote WHERE setor_cod = %s AND quadra_cod = %s AND lote_cod = %s
                                """, (inscri[0], inscri[1], inscri[2], inscri[0], inscri[1], inscri[2]))
                    recset2 = cur.fetchall()

                    # Se encontramos algum resultado, iteramos através deles
                    if len(recset2) > 0:
                        for rec2 in recset2:
                            # Para cada resultado, procuramos o endereco correspondente
                            cur.execute(
                                'select * from dado_antigo.endereco where id=%s', (rec2[0],))
                            endereco = cur.fetchall()
                            # Se o endereco for encontrado, procuramos o logradouro correspondente
                            if len(endereco) > 0:
                                cur.execute(
                                    'select id from dado_novo.logradouro where cod = %s', (endereco[0][2],))
                                log = cur.fetchall()

                                # Se o logradouro for encontrado, inserimos um novo endereco na base de dados, caso contrário, imprimimos uma mensagem
                                if len(log) > 0:
                                    cur.execute(
                                        'INSERT INTO dado_novo.endereco(id, logradouro, bairro, numero, complemento,\
                                        apartamento, loteamento) \
                                        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) do nothing',
                                        (endereco[0][0], log[0][0], endereco[0][3], endereco[0][5], endereco[0][6],
                                         endereco[0][7], endereco[0][8]))
                                else:
                                    cur.execute(
                                        'INSERT INTO dado_novo.endereco(id, bairro, numero, complemento,\
                                        apartamento, loteamento) VALUES ( %s,%s,%s,%s,%s,%s)',
                                        (endereco[0][0],  endereco[0][3], endereco[0][5], endereco[0][6],
                                         endereco[0][7], endereco[0][8]))
                                    print(
                                        f"Logradouro sem geometria. cod: {endereco[0][2]} e nome {endereco[0][1]} lote: {inscri[0]}-{inscri[1]}-{inscri[2]}")
                            else:
                                # Se o endereco nao for encontrado, atualizamos o nome na tabela lote
                                cur.execute(
                                    "update public.lote set name=%s \
                                    where name=%s", (f'{inscri[0]}-{inscri[1]}-{inscri[2]}-End', f'{inscri[0]}-{inscri[1]}-{inscri[2]}'))
                                print(
                                    f"Endereco do lote {inscri[0]}-{inscri[1]}-{inscri[2]} nao encontrado ")

                    else:
                        # Se nenhuma correspondência for encontrada na consulta UNION, imprimimos uma mensagem
                        print('Nao encontrado lote no setor, quadra e lote ' +
                              str(inscri[0])+"-"+str(inscri[1]))
