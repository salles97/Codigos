
def cadastra_testada(reduzido, cur):
    try:
        cur.execute(
            "SELECT count(*) FROM dado_novo.testada WHERE lote_id = %s", (reduzido,))
        cont = cur.fetchone()[0]

        cur.execute("SELECT ta.geom FROM public.testada ta, dado_novo.lote l "
                    "WHERE ST_CONTAINS(ST_BUFFER(l.geom,2), ta.geom) AND l.id = %s", (reduzido,))
        testada_geoms = cur.fetchall()

        cur.execute(
            "SELECT logradouro_cod FROM dado_antigo.endereco WHERE id = %s", (reduzido,))
        logradouro_cod = cur.fetchone()[0]

        for testada_geom in testada_geoms:
            cur.execute("SELECT e.id, e.logradouro_nome, e.logradouro_id  FROM dado_novo.eixo e "
                        "WHERE ST_Distance(ST_Centroid(%s),ST_Buffer(e.geom,1,'endcap=flat join=round')) < 100 "
                        "ORDER BY ST_Distance(ST_Centroid(%s),ST_Buffer(e.geom,1,'endcap=flat join=round')) LIMIT 1", (testada_geom, testada_geom))
            eixo = cur.fetchone()

            cont = 1
            if eixo and eixo['logradouro_id'] == logradouro_cod:
                face = 1
            else:
                cont += 1
                face = cont

            cur.execute("INSERT INTO dado_novo.testada (lote_id, eixo_id, geom, face, extensao) "
                        "VALUES (%s, %s, %s, %s, ST_length(%s))", (reduzido, eixo['id'], testada_geom, face, testada_geom))
        return True

    except Exception as e:
        print('Erro ao executar consulta:', e)
        return False
