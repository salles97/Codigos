import pandas as pd

def logradouros(cur):
    # Busca logradouros que possam causar conflitos de interseção
    cur.execute('''
        SELECT l.geom, l.name, ll.nome 
        FROM public.logradouro AS l
        JOIN dado_novo.logradouro AS ll 
        ON ST_Intersects(l.geom, ll.geom) 
        AND ST_Length(ST_Intersection(l.geom, ll.geom)) > 0
    ''')
    intersect_logradouro = cur.fetchall()
    
    if intersect_logradouro:
        print('------------ Logradouros que interseccionam geometrias já existentes')
        for conflict in intersect_logradouro:
            print(f'{conflict[1]} intersecciona com {conflict[2]}')
        return  # Retorna para evitar continuar se houver conflitos

    # Obtém os logradouros da tabela public
    cur.execute('SELECT ST_AsText(geom) as geom, name FROM public.logradouro')
    recset = cur.fetchall()

    # Converte os dados em um DataFrame
    df = pd.DataFrame(recset, columns=['geom', 'name'])

    # Remove substrings específicas do nome dos logradouros (como "(1)", "(2)", etc.)
    suffixes_to_remove = [' (1)', ' (2)', ' (3)', ' (4)', ' (5)', ' (6)', ' (7)']
    df['name'] = df['name'].apply(lambda x: x[:-4] if x.endswith(tuple(suffixes_to_remove)) else x)

    # Remove espaços em branco no final e normaliza para letras maiúsculas
    df['name'] = df['name'].str.strip().str.upper()

    # Remove a substring de geometria
    df['geom'] = df['geom'].str.replace('MULTILINESTRING\(', '', regex=True).str[:-1]

    # Itera sobre cada logradouro e executa as operações necessárias
    for index, row in df.iterrows():
         # Caso o nome seja 'RUA SEM NOME', insira diretamente sem buscar em dado_antigo
        if row['name'].upper() == 'RUA SEM NOME':
            cur.execute('insert into dado_novo.logradouro (geom, nome, cod) values (%s, %s, NULL)',
                        ('SRID=31983;MULTILINESTRING(' + str(row['geom']) + ')', row['name']))
            print(f'{row["name"]} inserido diretamente como "RUA SEM NOME"')
            continue
        
        logradouro_nome = row['name']

        # Verifica se o logradouro já está cadastrado na base dado_novo
        cur.execute('''
            SELECT id, ST_AsText(geom)
            FROM dado_novo.logradouro
            WHERE nome = %s
        ''', (logradouro_nome,))
        recset = cur.fetchall()

        # Atualiza a geometria se já estiver cadastrado
        if recset:
            # Atualiza a geometria concatenando com a geometria existente
            new_geom = f'SRID=31983;{recset[0][1][:-1]},{row["geom"]})'
            cur.execute('UPDATE dado_novo.logradouro SET geom = %s WHERE id = %s', (new_geom, recset[0][0]))
            #  cur.execute('update dado_novo.logradouro set geom=%s where id=%s',
            #             ('SRID=31983;'+recset[0][1][:-1]+','+str(row['geom'])+')', str(recset[0][0])))
        else:
            # Se não estiver cadastrado, busca no dado_antigo (concatenando tipo e nome)
            cur.execute('''
                SELECT id, nome, cod
                FROM dado_antigo.logradouro
                WHERE CONCAT(tipo, ' ', nome) = %s
            ''', (logradouro_nome,))
            end = cur.fetchall()

            if end:
                # Insere o logradouro no dado_novo com o código do dado_antigo
                cur.execute('''
                    INSERT INTO dado_novo.logradouro (geom, nome, cod)
                    VALUES (%s, %s, %s)
                ''', (f'SRID=31983;MULTILINESTRING({row["geom"]})', row['name'], end[0][2]))
            else:
                # Insere com código NULL se não encontrado em dado_antigo
                cur.execute('''
                    INSERT INTO dado_novo.logradouro (geom, nome, cod)
                    VALUES (%s, %s, NULL)
                ''', (f'SRID=31983;MULTILINESTRING({row["geom"]})', row['name']))
                print(f'{row["name"]} não encontrado no dado_antigo. Inserido com código NULL.')

    print("------- Fim dos Logradouros -------")
