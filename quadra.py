import pandas as pd


def quadras(cur):
    # Busca todas as quadras na fonte pública
    cur.execute("select name, geom from public.quadra")
    recset = cur.fetchall()

    # Converte o resultado para um DataFrame e realiza limpezas necessárias
    df = pd.DataFrame(recset, columns=['name', 'geom'])
    df['name'] = df['name'].str.replace(r"\(1[0]?\)$", "", regex=True)

    # Itera sobre as linhas do DataFrame
    for index, row in df.iterrows():
        print(row['name'])
        # Se o nome da quadra possui um código e um setor
        if len(row['name'].split('-')) == 2:
            # Extrai o código e o setor
            setor, cod = row['name'].split('-')
            # Verifica se já existe uma quadra com o mesmo código e setor no banco de dados
            cur.execute(
                "select geom from dado_novo.quadra where setor_cod=%s and cod=%s", (setor, cod))
            recset = cur.fetchall()
            if recset and recset[0][0] is not None:
                # Se existir, une as geometrias da quadra existente com a nova
                cur.execute(
                    "select ST_Multi(ST_UNION(%s::geometry, %s::geometry))", (recset[0][0], row['geom']))
                new_geom = cur.fetchall()[0][0]
                # E atualiza a geometria da quadra existente
                cur.execute(
                    "update dado_novo.quadra set geom=%s where setor_cod=%s and cod=%s", (new_geom, setor, cod))
            else:
                # Se não existir, insere a nova quadra
                cur.execute(
                    "insert into dado_novo.quadra (cod, setor_cod, distrito_cod, geom) values (%s, %s, 1, %s)", (cod, setor, row['geom']))
        else:
            # Se o nome da quadra não possui um código e um setor, insere na tabela de revisão
            cur.execute(
                "insert into to_review.quadra (name,geom) values (%s, %s)", (row['name'], row['geom']))
