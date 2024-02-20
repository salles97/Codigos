import pandas as pd


def logradouros(cur):
    # Busca ver se o logradouro que vai ser inserido irá dar conflito com algum já existente.

    # A função ST_Length irá ver o comprimento do retorno de ST_Intersection e se for maior que 0 é uma linha e não somente um ponto.
    # A função ST_Intersection busca a intersecção entre duas geometrias, se as ruas forem um cruzamento o retorno é um ponto apenas.
    cur.execute('''
        select l.geom, l.name, ll.nome 
        from public.logradouro as l, dado_novo.logradouro as ll 
        where st_intersects(l.geom, ll.geom) and ST_Length(ST_Intersection(l.geom, ll.geom)) > 0
    ''')
    intersectLogradouro = cur.fetchall()
    if len(intersectLogradouro) > 0:
        print('------------ Logradouros que interseccionam geometrias ja existentes')
        for conflict in intersectLogradouro:
            print(f'{conflict[1]} intersecciona com {conflict[2]}')

    cur.execute('select ST_AsText(geom) as geom, name from public.logradouro')
    recset = cur.fetchall()

    # Converte o dado para DataFrame
    df = pd.DataFrame(recset, columns=['geom', 'name'])

    # Exclui as seguintes substrings do nome do logradouro
    df.loc[df['name'].str.endswith(tuple([' (1)', ' (2)', ' (3)', ' (4)', ' (5)', ' (6)', ' (7)'])), [
        'name']] = df['name'].str[:-4]
    # remover espaços em branco no final.
    df.loc[df['name'].str.endswith(' '), ['name']] = df['name'].str[:-1]
    # Exclui a substring do campo de geometria
    df['geom'] = df['geom'].str.replace(
        'MULTILINESTRING\(', '', regex=True).str[:-1]   

    # print('Logradouros não encontrados no cadastro da prefeitura.')
    for index, row in df.iterrows():
      # Busca saber se o logradouro já esta cadastrado em dado_novo, pois alguns possuem 'multi' partes e dever ser unidos em só registro.
      # Busca ver também se existe apenas um código para este logradouro na base da prefeitura.
        cur.execute('select id, ST_AsText(geom) from dado_novo.logradouro where nome=%s and (SELECT count(*) FROM dado_antigo.logradouro where nome = %s) = 1',
                    (row['name'], row['name']))
        recset = cur.fetchall()

      # Se já cadastrado, atualiza a geometria do logradouro
        if len(recset) > 0:
            cur.execute('update dado_novo.logradouro set geom=%s where id=%s',
                        ('SRID=31983;'+recset[0][1][:-1]+','+str(row['geom'])+')', str(recset[0][0])))
        # Se não, cadastra!
        else:
            cur.execute(
                'SELECT * FROM dado_antigo.logradouro WHERE nome=%s', (row['name'],))
            end = cur.fetchall()
            # O rótulo do logradouro deve existir em dado_antigo, caso contrário há um erro com a rotulagem ou falta o registro na base extraida da prefeitura.
            if len(end) > 0:
                cur.execute('insert into dado_novo.logradouro (geom, nome, cod) values (%s, %s, %s)',
                            ('SRID=31983;MULTILINESTRING(' + str(row['geom']) + ')', row['name'], str(end[0][2])))
            # Logradouros que não são encontrados na base da prefeitura são carregados com codigo null, e devem ser corrigidos futuramente.
            else:
                cur.execute('insert into dado_novo.logradouro (geom, nome, cod) values (%s, %s, NULL)',
                            ('SRID=31983;MULTILINESTRING(' + str(row['geom']) + ')', row['name']))
                print(row['name'])

    print("------- Fim dos Logradouros -------")
