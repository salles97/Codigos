def clear_public_geoms(cur, name):
    cur.execute(
        'DELETE FROM public.eixo WHERE geom in = (SELECT geom FROM dado_novo.eixo)')
    cur.execute(
        'DELETE FROM public.logradouro WHERE geom in = (SELECT geom FROM dado_novo.logradouro)')
    cur.execute(
        'DELETE FROM public.quadra WHERE geom in = (SELECT geom FROM dado_novo.quadra)')
    cur.execute(
        'DELETE FROM public.lote WHERE geom in = (SELECT geom FROM dado_novo.lote)')
    cur.execute(
        'DELETE FROM public.area_coberta WHERE geom in = (SELECT geom FROM dado_novo.area_coberta)')
    cur.execute(
        'DELETE FROM public.benfeitoria WHERE geom in = (SELECT geom FROM dado_novo.benfeitoria)')
    cur.execute(
        'DELETE FROM public.area_especial WHERE geom in = (SELECT geom FROM dado_novo.area_especial)')
    cur.execute(
        'DELETE FROM public.testada WHERE geom in = (SELECT geom FROM dado_novo.testada)')

    cur.execute(
        'DELETE FROM public.eixo WHERE geom in = (SELECT geom FROM to_review.eixo)')
    cur.execute(
        'DELETE FROM public.logradouro WHERE geom in = (SELECT geom FROM to_review.logradouro)')
    cur.execute(
        'DELETE FROM public.quadra WHERE geom in = (SELECT geom FROM to_review.quadra)')
    cur.execute(
        'DELETE FROM public.lote WHERE geom in = (SELECT geom FROM to_review.lote)')
    cur.execute(
        'DELETE FROM public.area_coberta WHERE geom in = (SELECT geom FROM to_review.area_coberta)')
    cur.execute(
        'DELETE FROM public.benfeitoria WHERE geom in = (SELECT geom FROM to_review.benfeitoria)')
    cur.execute(
        'DELETE FROM public.area_especial WHERE geom in = (SELECT geom FROM to_review.area_especial)')
    cur.execute(
        'DELETE FROM public.testada WHERE geom in = (SELECT geom FROM to_review.testada)')
