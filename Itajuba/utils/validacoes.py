
# As validações Geometricas tem como propósito identificar incosistencias com os dados geometricos
# antes de associar aos registros da prefeitura
#
# Busca identificar:
# Lotes que não se encontram dentro de sua quadra
#   O rotulo da quadra, contempla seu codigo, assim como no rotulo do lote tem a sua quadra
#   Aqui se compara as geometrias de lote e quadras com base na combinação de setor e quadra contida em seus rotulos
#
# Areas Cobertas que não estão dentro de nenhum lote
#   Busca consultar espacialmente todas as coberturas que não estão dentro de nenhum lote
#
# Validar se algum logradouro está em cruzando com a geometria de alguma quadra
#
# Validar se algum eixo está em cruzando com a geometria de alguma quadra
#
# Validar se existe duplicações de geometria de testadas
#     Bem comum vir duplicado e causar muito ajuste manual
#
#
#

# Validações que nao tem como serem adiantas:
# Validação se cobertura nao possui relacionamento com nenhuma unidade
#
# Validação se Lote vago tem endereco e proprietario
#
# Validação se lote ocupado não tem endereco e proprietario
#
# Unidades sem Cobertura
#
# Coberturas sem Unidades
#
# Lote possui testada principal ou mais de uma
#
# Logradouros carregados não compatíveis com registros antigos
def validacoes_pre_carga(cur):
    cur.execute("SELECT name, geom FROM public.lote")
    lotes = cur.fetchall()

    for lote in lotes:
        validar_lote_e_quadra(lote['name'], cur)
        # Se o lote nao tiver a informação da quadra, apenas ver se estea contido em alguma quadra

    area_fora_lote(cur)
    validar_logradouros_eixos(cur)


def validar_lote_e_quadra(nome, cur):
    # Busca a quadra que o lote está
    cur.execute(
        'select q.name from public.lote as l, public.quadra as q where ( ST_Contains(q.geom, l.geom) and l.name = %s)', nome)
    quadra = cur.fetchone()
    partes = nome.split('-')
    if quadra:
        if len(partes) > 2:
            setor_cod = partes[0]
            quadra_cod = partes[1]

            quadra_setor_cod = quadra['name'].split('-')[0]
            codigo_quadra = quadra['name'].split('-')[1]

            if (setor_cod != quadra_setor_cod) or (quadra_cod != codigo_quadra):
                print(
                    'Lote %s em quadra incorreta, identificado na quadra %s', nome, quadra['name'])

    else:
        print('Lote em nenhuma quadra %s', nome)


def area_fora_lote(cur):
    # Regra Area Coberta dentro de um algum lote -- Valida se existe cobertura fora de lote.
    cur.execute(
        'select a.id from public.area_coberta as a where not exists (select name from public.lote as l where ST_Within(a.geom, l.geom))')
    are_coberta = cur.fetchall()
    if len(are_coberta) > 0:
        print('----REGRA AREA COBERTA INVALIDA Fora de lote----')
        for a in are_coberta:
            print('Problema na area_coberta: %s', str(a[0]))
    else:
        print('-----REGRA AREA COBERTA VALIDO-----')

    print('-----------------------------------')


def validar_logradouros_eixos(cur):
 # Regra Logradouro tocar quadra, lote ou area coberta
    cur.execute(
        'select r.id from public.logradouro as r where (exists (select * from public.quadra as q where ST_Intersects(q.geom, r.geom))) or (exists (select * from public.lote as l where ST_Intersects(l.geom, r.geom))) or (exists (select * from public.area_coberta as a where ST_Intersects(a.geom, r.geom)))')
    log = cur.fetchall()
    if len(log) > 0:
        print('------REGRA LOGRADOURO INVALIDA------')
        for l in log:
            print('Problema no logradouro:' + str(l[0]))
    else:
        print('-------REGRA LOGRADOURO VALIDA-------\n\n')

    print('\n')

    # Regra Eixo tocar quadra, lote, area coberte ou outros eixos
    cur.execute(
        'select r.id from public.eixo as r where (exists (select * from public.quadra as q where ST_Intersects(q.geom, r.geom))) or (exists (select * from public.lote as l where ST_Intersects(l.geom, r.geom))) or (exists (select * from public.area_coberta as a where ST_Intersects(a.geom, r.geom))) or (exists (select * from dado_novo.eixo as a where ST_Intersects(a.geom, r.geom) and (r.id <> a.id) ))')
    log = cur.fetchall()
    if len(log) > 0:
        print('--------REGRA EIXO INVALIDA--------')
        for l in log:
            print('Problema no eixo:' + str(l[0]))
    else:
        print('---------REGRA EIXO VALIDA---------')

    print('\n')

# def validacao_geometrica(cur):
    # Verifica se a cobertura associada as unidades estão no lote dela
    # cur.execute(
    #     'select a.id from dado_novo.lote as l, dado_novo.area_coberta as a, dado_novo.unidade_imobiliaria as u,\
    #         dado_novo.unidade_cobertura as c where l.id=u.lote_id and u.id=c.unidade_id and a.id=c.area_coberta_id\
    #             and (not ST_Within(a.geom, l.geom))')
    # area_coberta = cur.fetchall()
    # if len(area_coberta) > 0:
    #     print('----REGRA AREA COBERTA INVALIDA----')
    #     for a in area_coberta:
    #         print('Problema na area_coberta:' + str(a[0]))
    # else:
    #     print('-----REGRA AREA COBERTA VALIDO-----')


# Regras de validação de Cadastral
def validacoes_pos_carga(cur):
    # Regra lote
    #     cur.execute(
    #         'select id from dado_novo.lote where ((vago=\'n\') and (proprietario_id is not null or endereco_id is not null)) or ((vago=\'s\') and (proprietario_id is null or endereco_id is null)) order by id')
    #     cur.execute(
    #         'select id, setor_cod, quadra_cod,lote_cod from dado_novo.lote where ((vago=\'n\') and (proprietario_id is not null or endereco_id is not null))  order by id')
    #     lote = cur.fetchall()
    #     if len(lote) > 0:
    #         print('--------REGRA LOTE INVALIDA - Lote Ocupado e com dados de proprietario ou de endereço --------')
    #         for l in lote:
    # #             print('Problema no lote:' + str(l[0]) +', '+str(l[1])+'-'+str(l[2])+'-'+str(l[3]))
    #             print(str(l[0]))
    #     else:
    #         print('---------REGRA LOTE VALIDO - Lotes ocupados desviculados de proprietario e de endereço!---------')

    #     print('-----------------------------------')

  # Regra lote
    # cur.execute(
    #     'select id from dado_novo.lote where ((vago=\'s\') and (proprietario_id is null or endereco_id is null)) order by id')
    # lote = cur.fetchall()
    # if len(lote) > 0:
    #     print('--------REGRA LOTE INVALIDA - Lote vago e sem dados de proprietario e/ou endereço --------')
    #     for l in lote:
    #         print('Problema no lote:' + str(l[0]))
    # else:
    #     print('---------REGRA LOTE VALIDO - lotes vagos com endereços e proprietarios! ---------')
    #     #print(cur.fetchall())

    # print('-----------------------------------')

    # Regra Lote e Testada -- Quais lotes tao sem testada principal
    cur.execute(
        'select v.id, v.setor_cod, v.quadra_cod, v.lote_cod from (SELECT l.id, l.setor_cod, l.quadra_cod, l.lote_cod ,t.face  FROM dado_novo.lote AS l LEFT JOIN\
        (select * from dado_novo.testada where face=1) AS t ON l.id = t.lote_id) as v where v.face is null order by v.id')
    lote_testada = cur.fetchall()
    if len(lote_testada) > 0:
        print('--REGRA (LOTE E TESTADA) INVALIDA -- Lotes sem testada principal--')
        for l in lote_testada:
            print('Problema no lote: ' + str(l[0]) + ' setor: '+str(
                l[1]) + ' quadra: '+str(l[2]) + ' lote: '+str(l[3]))
    else:
        print('--REGRA (LOTE E TESTADA) VALIDA--')
    print('-----------------------------------')

    # Regra mais de uma Testada Face 1
    cur.execute(
        'SELECT lote_id, count(*) FROM dado_novo.testada where face=1 GROUP BY lote_id, face having count(*) > 1 order by lote_id')
    testada = cur.fetchall()

    if len(testada) > 0:
        print('------REGRA TESTADA FACE INVALIDA - - MAIS DE UMA FACE 1 ------')
        for t in testada:
            print(' lote ' + str(t[0]) + ' possui ' +
                  str(t[1]) + ' testadas com Face 1')
    else:
        print('-------REGRA TESTADA FACE VALIDA-------')
    print('-----------------------------------')

    # Regra Unidade Imobiliaria
    cur.execute('select u.id, setor_cod, quadra_cod, lote_cod from dado_novo.unidade_imobiliaria as u where u.id not in \
    (select unidade_id from dado_novo.unidade_cobertura)')
    unidade_sem_cobertura = cur.fetchall()
    if len(unidade_sem_cobertura) > 0:
        print('-REGRA UNIDADE IMOBILIARIA INVALIDA -- Unidades sem Cobertura ')
        for u in unidade_sem_cobertura:
            inscri = str(u[1])+'-'+str(u[2])+'-'+str(u[3])
            print('Problema na unidade: ' + str(u[0])+' Inscrição: '+inscri)
    else:
        print('-REGRA UNIDADE IMOBILIARIA VALIDA -- Nenhuma Unidade sem cobertura')
    print('-----------------------------------')

    # Regra Eixo e Logradouro
    cur.execute('SELECT e.id, e.logradouro_id, e.logradouro_nome FROM dado_novo.eixo as e \
    LEFT JOIN dado_novo.logradouro as l ON e.logradouro_id = l.id WHERE e.logradouro_id is null')
    eixo = cur.fetchall()
    if len(eixo) > 0:
        print('--------REGRA EIXO INVALIDA -> logradouro nao identificado --------')
        for e in eixo:
            print('Eixo id: ' + str(e[0]) + ', nome: ' + str(e[2]))
    else:
        print('---------REGRA EIXO VALIDA -> logradouro nao identificado ---------')
    print('-----------------------------------')

    # Regra Endereço e Logradouro
    cur.execute('select id from dado_novo.endereco where logradouro is null')
    endereco = cur.fetchall()
    if len(endereco) > 0:
        print('------REGRA ENDERECO INVALIDA  -> logradouros nulos------')
        for e in endereco:
            print('Problema no endereco: ' + str(e[0]))
    else:
        print('-------REGRA ENDERECO VALIDA -> logradouros nulos-------')
    print('-----------------------------------')

    # Regra Area coberta associada a alguma unidade
    cur.execute(
        'select u.id from dado_novo.area_coberta as u where u.id not in \
        (select area_coberta_id from dado_novo.unidade_cobertura)')
    are_coberta = cur.fetchall()
    if len(are_coberta) > 0:
        print('-REGRA AREA COBERTA ASSOCIADA INVALIDA-')
        for a in are_coberta:
            print('Problema na area_coberta:' + str(a[0]))
    else:
        print('-REGRA AREA COBERTA VALIDO ASSOCIADA-')

    print('-----------------------------------')
