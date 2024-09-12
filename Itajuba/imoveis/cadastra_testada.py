def cadastra_testada(reduzido, cur, arquivo_log):
    try:
        cur.execute(
            "SELECT count(*) FROM dado_novo.testada WHERE lote_id = %s", (reduzido,))
        cont = cur.fetchone()[0]
        # arquivo_log.write(f"Número de testadas existentes para o lote {reduzido}: {cont}\n")

        cur.execute("SELECT ST_AsText(ta.geom) FROM public.testada ta, dado_novo.lote l "
                    "WHERE ST_CONTAINS(ST_BUFFER(l.geom,2), ta.geom) AND l.id = %s", (reduzido,))
        testada_geoms = cur.fetchall()
        # arquivo_log.write(f"Número de geometrias de testada encontradas para o lote {reduzido}: {len(testada_geoms)}\n")

        cur.execute(
            "SELECT logradouro_cod FROM dado_antigo.endereco WHERE id = %s", (reduzido,))
        logradouro_cod = cur.fetchone()[0]
        # arquivo_log.write(f"Código do logradouro para o lote {reduzido}: {logradouro_cod}\n")

        # Consulta para obter o logradouro_id correspondente ao logradouro_cod
        cur.execute(
            "SELECT id FROM dado_novo.logradouro WHERE cod = %s", (logradouro_cod,))
        result = cur.fetchone()
        logradouro_id_principal = result[0] if result else None
        # arquivo_log.write(f"ID do logradouro principal para o lote {reduzido}: {logradouro_id_principal}\n")

        for i, testada_geom in enumerate(testada_geoms):
            # arquivo_log.write(f"Processando testada {i+1} para o lote {reduzido}\n")
            # arquivo_log.write(f"Geometria da testada: {testada_geom[0][:100]}...\n")  # Mostra apenas os primeiros 100 caracteres

            cur.execute("""
                SELECT e.id, e.logradouro_nome, e.logradouro_id
                FROM dado_novo.eixo e
                WHERE ST_Distance(ST_Centroid(ST_GeomFromText(%s, 31983)), ST_Buffer(e.geom, 1, 'endcap=flat join=round')) < 100
                ORDER BY ST_Distance(ST_Centroid(ST_GeomFromText(%s, 31983)), ST_Buffer(e.geom, 1, 'endcap=flat join=round'))
                LIMIT 1
            """, (testada_geom[0], testada_geom[0]))
            eixo = cur.fetchone()

            # if eixo:
            #     # arquivo_log.write(f"Eixo encontrado para a testada {i+1}: id={eixo['id']}, logradouro_nome={eixo['logradouro_nome']}, logradouro_id={eixo['logradouro_id']}\n")
            # else:
            #     arquivo_log.write(f"Nenhum eixo encontrado para a testada {i+1}\n")
            #     continue

            cont = 1
            if eixo and eixo['logradouro_id'] == logradouro_id_principal:
                face = 1
            else:
                cont += 1
                face = cont

            # arquivo_log.write(f"Face determinada para a testada {i+1}: {face}\n")

            try:
                cur.execute("""
                    INSERT INTO dado_novo.testada (lote_id, eixo_id, geom, face, extensao)
                    VALUES (%s, %s, ST_GeomFromText(%s, 31983), %s, ST_length(ST_GeomFromText(%s, 31983)))
                """, (reduzido, eixo['id'], testada_geom[0], face, testada_geom[0]))
                # arquivo_log.write(f"Testada {i+1} inserida com sucesso para o lote {reduzido}\n")
            except Exception as e:
                arquivo_log.write(f"Erro ao inserir testada {i+1} para o lote {reduzido}: {e}\n")

        # arquivo_log.write(f"Processamento de testadas concluído para o lote {reduzido}\n")
        return True

    except Exception as e:
        arquivo_log.write(f'Erro ao executar consulta em cadastra_testada: {e}\n')
        return False
