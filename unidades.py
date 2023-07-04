
# Para cada lote cadastrado novo:

# Buscar na base da prefeitura as unidades imobiliaria do lote.
# Consulta espacialmente para saber se tem alguma coberturas (telhados ou piscinas) para este lote em public
#
#
# Possui coberturas e unidades?
#     Cadastra as unidades do lote em dado_novo
#     Carregas as geometrias das coberturas e/ou piscinas
#     Cadastra os relacionamentos de unidades e coberturas
#
# Possui coberturas e não possui unidades?
#     Cria unidade em dado_novo copiando info do lote
#     Carregas as geometrias das coberturas e/ou piscinas
#     Cadastra os relacionamentos de unidades e coberturas
#
#     Lote é predial ?
#           Não atualiza área construida das unidades
#     Lote não predial?
#           Atualiza a área construida do lote
#
# Não possue coberturas e possui unidades?
#     Cadastra as unidades do lote em dado_novo
#
#
# Não possui coberturas e não possui unidades?
#     Não faz nada


def unidades_imobiliarias(cur):
    condition = str(lotesNovos).replace('[', '').replace(']', '')
#     print(condition)
    cur.execute("select * from dado_novo.lote where id in (%s)" % condition)
#     cur.execute("select * from dado_novo.lote")
    lotes = cur.fetchall()
# Para cada lote cadastrado novo:
    for l in lotes:
        # Buscar na base da prefeitura as unidades imobiliaria do lote.
        cur.execute("select id, lote_id, setor_cod, quadra_cod, lote_cod, unidade_cod, proprietario_id, endereco_id from dado_antigo.unidade_imobiliaria \
      where setor_cod = " + str(l[4]) + " and quadra_cod = " + str(l[5]) + " and lote_cod = " + str(l[6]))
        unidadeImob = cur.fetchall()

# Consulta espacialmente para saber se tem alguma coberturas (telhados ou piscinas) para este lote em public
        # cur.execute("SELECT agg_Array(ac.geom) FROM public.")
#
# Possui coberturas e unidades?
#     Cadastra as unidades do lote em dado_novo
#     Carregas as geometrias das coberturas e/ou piscinas
#     Cadastra os relacionamentos de unidades e coberturas
#
# Possui coberturas e não possui unidades?
#     Cria unidade em dado_novo copiando info do lote
#     Carregas as geometrias das coberturas e/ou piscinas
#     Cadastra os relacionamentos de unidades e coberturas
#
#     Lote é predial ?
#           Não atualiza área construida das unidades
#     Lote não predial?
#           Atualiza a área construida do lote
#
# Não possue coberturas e possui unidades?
#     Cadastra as unidades do lote em dado_novo
#
#
# Não possui coberturas e não possui unidades?
#     Não faz nada


def unidades_imobiliarias(cur):
    condition = str(lotesNovos).replace('[', '').replace(']', '')
#     print(condition)
    cur.execute("select * from dado_novo.lote where id in (%s)" % condition)
#     cur.execute("select * from dado_novo.lote")
    lotes = cur.fetchall()
    cont = 0
    arquivo = open(entrega + '.txt', 'w')
    for l in lotes:
        #         print(l[0])
        if l[46] == 's':  # entra aqui se o lote for do tipo predial
            cur.execute("SELECT ac.id, ST_Area(ac.geom), ST_AsEWKT(ac.geom) FROM dado_novo.lote l, public.area_coberta ac "
                        "WHERE l.id = " + str(l[0]) + " AND ST_Contains(l.geom, ac.geom)")
            areaCoberta = cur.fetchall()
#             print(areaCoberta)
            cur.execute("SELECT ac.id, ST_Area(ac.geom), ST_AsEWKT(ac.geom) FROM dado_novo.lote l, public.benfeitoria ac "
                        "WHERE l.id = " + str(l[0]) + " AND ST_Contains(l.geom, ac.geom)")
            benfeitoria = cur.fetchall()

            bfId = []
            acId = []

            # predial com uma cobertura só
            if (len(areaCoberta) + len(benfeitoria)) == 1:
                for ac in areaCoberta:
                    cur.execute(
                        "SELECT id FROM dado_novo.area_coberta WHERE geom = \'" + ac[2] + "\'")
                    acId = cur.fetchone()
                    if acId is None:
                        cur.execute(
                            "insert into dado_novo.area_coberta (geom) values ('" + ac[2] + "') RETURNING id")
                        acId = cur.fetchone()
                for bf in benfeitoria:
                    cur.execute(
                        "SELECT id FROM dado_novo.benfeitoria WHERE geom = \'" + bf[2] + "\'")
                    bfId = cur.fetchone()
                    if bfId is None:
                        cur.execute(
                            "insert into dado_novo.benfeitoria (geom) values ('" + bf[2] + "') RETURNING id")
                        bfId = cur.fetchone()

                cur.execute(
                    "select id, lote_id, setor_cod, quadra_cod, lote_cod, unidade_cod, proprietario_id, endereco_id from dado_antigo.unidade_imobiliaria where setor_cod = " + str(
                        l[4]) + " and quadra_cod = " + str(l[5]) +
                    " and lote_cod = " + str(l[6]))
                unidadeImob = cur.fetchall()

                if acId != []:
                    for ui in unidadeImob:
                        cur.execute(
                            "insert into dado_novo.unidade_imobiliaria (id,lote_id,setor_cod,quadra_cod,lote_cod,"
                            "unidade_cod,proprietario_id,endereco_id"
                            ") values (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (ui[0], ui[1], ui[2], ui[3], ui[4], ui[5], ui[6], ui[7]))
                        cur.execute("insert into dado_novo.unidade_cobertura (unidade_id, area_coberta_id) "
                                    "values (%s,%s)", (ui[0], acId))

                if bfId != []:
                    for ui in unidadeImob:
                        cur.execute(
                            "insert into dado_novo.unidade_imobiliaria (id,lote_id,setor_cod,quadra_cod,lote_cod,"
                            "unidade_cod,proprietario_id,endereco_id"
                            ") values (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (ui[0], ui[1], ui[2], ui[3], ui[4], ui[5], ui[6], ui[7]))
                        cur.execute("insert into dado_novo.unidade_benfeitoria (unidade_id, benfeitoria_id) "
                                    "values (%s,%s)", (ui[0], bfId))

            # predial com mais de uma cobertura
            else:
                #                 for ac in areaCoberta:
                #                     cur.execute(
                #                         "insert into dado_novo.area_coberta (geom) values ('" + ac[2] + "') RETURNING id")
                #                 for bf in benfeitoria:
                #                     cur.execute(
                #                         "insert into dado_novo.benfeitoria (geom) values ('" + bf[2] + "') RETURNING id")
                for ac in areaCoberta:
                    cur.execute(
                        "SELECT id FROM dado_novo.area_coberta WHERE geom = \'" + ac[2] + "\'")
                    acId = cur.fetchone()
                    if acId is None:
                        cur.execute(
                            "insert into dado_novo.area_coberta (geom) values ('" + ac[2] + "') RETURNING id")
                        acId = cur.fetchone()
                for bf in benfeitoria:
                    cur.execute(
                        "SELECT id FROM dado_novo.benfeitoria WHERE geom = '" + bf[2]+"'")
                    bfId = cur.fetchone()
                    if bfId is None:
                        cur.execute(
                            "insert into dado_novo.benfeitoria (geom) values ('" + bf[2] + "') RETURNING id")
                        bfId = cur.fetchone()

                cur.execute(
                    "select id, lote_id, setor_cod, quadra_cod, lote_cod, unidade_cod, proprietario_id, endereco_id from dado_antigo.unidade_imobiliaria where setor_cod = " + str(
                        l[4]) + " and quadra_cod = " + str(l[5]) +
                    " and lote_cod = " + str(l[6]))
                unidadeImob = cur.fetchall()

                for ui in unidadeImob:
                    cur.execute(
                        "insert into dado_novo.unidade_imobiliaria (id,lote_id,setor_cod,quadra_cod,lote_cod,"
                        "unidade_cod,proprietario_id,endereco_id"
                        ") values (%s,%s,%s,%s,%s,%s,%s,%s)",
                        (ui[0], ui[1], ui[2], ui[3], ui[4], ui[5], ui[6], ui[7]))
                    arquivo.write(str(ui[0]) + " - ")

                print("Tem q tratar na mão kkkk")

        else:  # entra aqui se o lote não for predial
            if l[45] == 'n':  # se nao for vago

                cur.execute(
                    "insert into dado_novo.area_coberta (geom) "
                    "SELECT ST_AsEWKT(ac.geom)"
                    "FROM dado_novo.lote l, public.area_coberta ac  "
                    "WHERE  l.id=" + str(l[0]) +
                    " AND  ST_Contains(l.geom, ac.geom)"
                    "RETURNING id")
                acId = cur.fetchall()

                cur.execute(
                    "insert into dado_novo.benfeitoria (geom) "
                    "SELECT ST_AsEWKT(ac.geom)"
                    "FROM dado_novo.lote l, public.benfeitoria ac  "
                    "WHERE  l.id=" + str(l[0]) +
                    " AND  ST_Contains(l.geom, ac.geom)"
                    "RETURNING id")
                bfId = cur.fetchall()

                # select unidades imobiliarias para inserir
                cur.execute(
                    "select id, lote_id, setor_cod, quadra_cod, lote_cod, unidade_cod, proprietario_id, endereco_id\
                    from dado_antigo.unidade_imobiliaria where setor_cod = " + str(
                        l[4]) + " and quadra_cod = " + str(l[5]) +
                    " and lote_cod = " + str(l[6]) + "order by id asc")
                unidadeImob = cur.fetchall()

#                 Se nao retornar nenhuma unidade, cadastra ela.
                if len(unidadeImob) == 0:
                    cur.execute("insert into dado_novo.unidade_imobiliaria (id,lote_id,setor_cod,quadra_cod,lote_cod,"
                                "unidade_cod,proprietario_id,endereco_id) values "
                                "(" + str(l[0]) + "," + str(l[0]) + "," + str(l[4]) + "," + str(l[5]) + "," + str(l[6]) +
                                ", 1 ," + str(l[1]) + "," + str(l[2]) + ")")
                    cur.execute(
                        "update dado_novo.lote set proprietario_id = %s, endereco_id = %s where id = %s", (None, None, l[0]))
                    if acId != None:
                        for a in acId:
                            cur.execute("insert into dado_novo.unidade_cobertura (unidade_id, area_coberta_id) "
                                        "values (%s,%s)", (l[0], a))
                    if bfId != None:
                        for b in bfId:
                            cur.execute("insert into dado_novo.unidade_benfeitoria (unidade_id, benfeitoria_id) "
                                        "values (%s,%s)", (l[0], b))

                else:
                    #                     print('Lote com uma ou mais unidade Imobiliaria')
                    for ui in unidadeImob:
                        cur.execute(
                            "select id from dado_novo.lote where id="+str(ui[1]))
                        checkLote = cur.fetchall()

                        if len(checkLote) > 0:
                            cur.execute(
                                'select * from dado_novo.endereco where id=' + str(ui[7]))
                            endereco = cur.fetchall()

#                             print(endereco)
                            if len(endereco) > 0:
                                cur.execute(
                                    "insert into dado_novo.unidade_imobiliaria (id,lote_id,setor_cod,quadra_cod,lote_cod,"
                                    "unidade_cod,proprietario_id,endereco_id) values "
                                    "(" + str(ui[0]) + "," + str(ui[1]) + "," + str(ui[2]) + "," + str(ui[3]) + "," + str(
                                        ui[4]) + "," + str(ui[5]) + "," + str(ui[6]) + "," + str(ui[7]) + ")")
                                if acId != None:
                                    for a in acId:
                                        cur.execute("insert into dado_novo.unidade_cobertura (unidade_id, area_coberta_id) "
                                                    "values (%s,%s)", (ui[0], a))
                                if bfId != None:
                                    for b in bfId:
                                        cur.execute("insert into dado_novo.unidade_benfeitoria (unidade_id, benfeitoria_id) "
                                                    "values (%s,%s)", (ui[0], b))

                                cur.execute(
                                    "update dado_novo.lote set proprietario_id = %s, endereco_id = %s \
                                    where id = %s", (None, None, l[0]))

                            else:
                                cur.execute(
                                    'select * from dado_antigo.endereco where id=' + str(ui[7]))
                                enderecoAnt = cur.fetchall()
                                if len(enderecoAnt) > 0:
                                    print(
                                        'Endereço em dado_novo sem cadastro ' + str(ui[7]))
                                else:
                                    print(
                                        'Endereço nao encontrado em lugar algum '+str(ui[7]))

            # se for vago

            else:
                cur.execute(
                    "select * from dado_antigo.unidade_imobiliaria where setor_cod = " + str(
                        l[4]) + " and quadra_cod = " + str(l[5]) +
                    " and lote_cod = " + str(l[6]) + " and unidade_cod = 1")
                unidadeImob = cur.fetchall()
                if len(unidadeImob) > 0:
                    for ui in unidadeImob:
                        #                         cur.execute('select * from dado_novo.endereco where id=' + str(ui[7]))
                        #                         endereco = cur.fetchall()
                        #                             print(endereco)
                        #                         if len(endereco) > 0:
                        cur.execute(
                            "update dado_novo.lote set endereco_id = %s, proprietario_id = %s where id = %s",
                            (ui[2], ui[1], l[0]))
#                         else:
#                             cur.execute(
#                             'select * from dado_antigo.endereco where id=' + str(ui[7]))
#                             enderecoAnt = cur.fetchall()
#                             if len(enderecoAnt) > 0:
#                                 print('Endereço em dado_novo sem cadastro '+ str(ui[7]))
#                             else:
#                                 print('Endereço nao encontrado em lugar algum '+str(ui[7]))
                else:
                    cur.execute(
                        "select endereco_id,proprietario_id from dado_antigo.lote where setor_cod = " + str(
                            l[4]) + " and quadra_cod = " + str(l[5]) +
                        " and lote_cod = " + str(l[6]) + "")
                    endprop = cur.fetchall()
                    cur.execute(
                        'select * from dado_novo.endereco where id=' + str(endprop[0][0]))
                    endereco = cur.fetchall()
#                             print(endereco)
                    if len(endereco) > 0:
                        cur.execute(
                            "update dado_novo.lote set endereco_id = %s, proprietario_id = %s where id = %s",
                            (endprop[0][0], endprop[0][1], l[0]))
                    else:
                        cur.execute(
                            'select * from dado_antigo.endereco where id=' + str(endprop[0][0]))
                        enderecoAnt = cur.fetchall()
                        if len(enderecoAnt) > 0:
                            print('Endereço em dado_novo sem cadastro ' +
                                  str(endprop[0][0]))
                        else:
                            print(
                                'Endereço nao encontrado em lugar algum ' + str(endprop[0][0]))

    arquivo.close()
