from shapely import wkb
from shapely.geometry import LineString

def eixos(cur):
    # Seleciona todos os eixos do banco de dados.
    cur.execute('SELECT geom, name, id FROM public.eixo')
    eixos = cur.fetchall()

    # Variável para contar quantos eixos nao têm logradouro correspondente.
    nao_encontrado = 0

    # Processa cada eixo individualmente.
    for eixo in eixos:
        # Primeira tentativa: sem buffer.
        cur.execute("""
            SELECT l.id, l.nome, ST_Length(ST_Intersection(l.geom, e.geom)) / ST_Length(e.geom) AS overlap
            FROM dado_novo.logradouro l, public.eixo e
            WHERE ST_Intersects(l.geom, e.geom) AND e.id = %s
            ORDER BY overlap DESC LIMIT 1
        """, (eixo[2],))
        logName = cur.fetchall()

        # Verifica se encontrou um logradouro correspondente.
        if logName:
            # Insere o eixo com referência ao logradouro correspondente.
            cur.execute("""
                INSERT INTO dado_novo.eixo (geom, logradouro_nome, logradouro_id) 
                VALUES (%s, %s, %s)
            """, (str(eixo[0]), str(logName[0][1]), str(logName[0][0])))
        else:
            # Segunda tentativa: aplica um buffer de 0.5 metros ao eixo.
            print(f'Logradouro nao encontrado para eixo {eixo[1]}, aplicando buffer de 0.5 metros.')
 

            # Nova tentativa com o eixo bufferizado.
            cur.execute("""
            SELECT l.id, l.nome, ST_Length(ST_Intersection(l.geom, ST_Buffer(e.geom, 0.5))) / ST_Length(e.geom) AS overlap
            FROM dado_novo.logradouro l, public.eixo e
            WHERE ST_Intersects(l.geom, e.geom) AND e.id = %s
            ORDER BY overlap DESC LIMIT 1
        """, (eixo[2],))
            logName_buffer = cur.fetchall()

            # Se encontrar com o buffer, insere.
            if logName_buffer:
                cur.execute("""
                    INSERT INTO dado_novo.eixo (geom, logradouro_nome, logradouro_id) 
                    VALUES (%s, %s, %s)
                """, (str(eixo[0]), str(logName_buffer[0][1]), str(logName_buffer[0][0])))
            else:
                # Caso nao encontre nem com o buffer, insere sem logradouro associado.
                nao_encontrado += 1
                if nao_encontrado == 1:
                    print('Nenhum logradouro encontrado para os eixos:')

                print(eixo[1])

                cur.execute("""
                    INSERT INTO dado_novo.eixo (geom, logradouro_nome, logradouro_id) 
                    VALUES (%s, %s, NULL)
                """, (str(eixo[0]), str(eixo[1])))

    print(f"Processo de insercao de eixos finalizado. Eixos sem logradouro: {nao_encontrado}")
    print("----------Fim eixos-----------------")
