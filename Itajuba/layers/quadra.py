import pandas as pd
import re

def quadras(cur):
    # Busca todas as quadras na fonte pública
    cur.execute("SELECT name, geom FROM public.quadra")
    recset = cur.fetchall()

    # Converte o resultado para um DataFrame e realiza limpezas necessárias
    df = pd.DataFrame(recset, columns=['name', 'geom'])
    
    # Regex para extrair setor_cod e quadra_cod, considerando (n) opcional e Q.sem_cod
    regex = re.compile(r'(\d+)-(?:(\d+)|Q_s\.cad)\s*(?:\(\d+\))?$')

    # Itera sobre as linhas do DataFrame
    for index, row in df.iterrows():
        print(row['name'])
        match = regex.match(row['name'])
        if match:
            setor = int(match.group(1))
            cod = match.group(2)
            
            if cod == None:  # Caso Q.sem_cod
                # Insere na tabela to_review.quadra
                cur.execute(
                    "INSERT INTO to_review.quadra (name, geom) VALUES (%s, %s)", (row['name'], row['geom']))
            else:
                cod = int(cod)
                # Verifica se já existe uma quadra com o mesmo codigo e setor no banco de dados
                cur.execute(
                    "SELECT geom FROM dado_novo.quadra WHERE setor_cod=%s AND cod=%s", (setor, cod))
                recset = cur.fetchall()

                if recset and recset[0][0] is not None:
                    # Se existir, une as geometrias da quadra existente com a nova
                    cur.execute(
                        "SELECT ST_Multi(ST_UNION(%s::geometry, %s::geometry))", (recset[0][0], row['geom']))
                    new_geom = cur.fetchall()[0][0]
                    # E atualiza a geometria da quadra existente
                    cur.execute(
                        "UPDATE dado_novo.quadra SET geom=%s WHERE setor_cod=%s AND cod=%s", (new_geom, setor, cod))
                else:
                    # Se nao existir, insere a nova quadra
                    cur.execute(
                        "INSERT INTO dado_novo.quadra (cod, setor_cod, distrito_cod, geom) VALUES (%s, %s, 1, %s)", (cod, setor, row['geom']))
        else:
            # Se o nome da quadra nao corresponde ao padrao esperado, insere na tabela de revisao
            cur.execute(
                "INSERT INTO to_review.quadra (name, geom) VALUES (%s, %s)", (row['name'], row['geom']))

    print("----------Fim quadras-----------------")
