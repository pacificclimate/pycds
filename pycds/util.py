from sqlalchemy import not_, and_, or_

from pycds import ObsWithFlags

# from http://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query
def compile_query(statement, bind=None):
    """
    print a query, with values filled in
    for debugging purposes *only*
    for security, you should always separate queries from their values
    please also note that this function is quite slow
    """
    import sqlalchemy.orm
    if isinstance(statement, sqlalchemy.orm.Query):
        if bind is None:
            bind = statement.session.get_bind(
                statement._mapper_zero_or_none()
                )
            statement = statement.statement
        elif bind is None:
            bind = statement.bind 

        dialect = bind.dialect
        compiler = statement._compiler(dialect)
        class LiteralCompiler(compiler.__class__):
            def visit_bindparam(
                    self, bindparam, within_columns_clause=False, 
                    literal_binds=False, **kwargs
            ):
                return super(LiteralCompiler, self).render_literal_bindparam(
                    bindparam, within_columns_clause=within_columns_clause,
                    literal_binds=literal_binds, **kwargs
                )

    compiler = LiteralCompiler(dialect, statement)
    return compiler.process(statement)

def orm_station_table(sesh, stn_id, raw=True):
    '''Construct a 'station table' i.e. a table such that each row corresponds to a single timestep
       and each column corresponds to a separate variable or flag

       :param sesh: sqlalchemy session
       :param stn_id: id corresponding to meta_station.station_id or Station.id
       :type stn_id: int
       :param raw: Should this query be for raw observations? Setting this to False will fetch climatologies.
       :type raw: bool
       :rtype: :py:class:`sqlalchemy.orm.query.Query`
    '''
    if raw:
        raw_filter = not_(and_(ObsWithFlags.cell_method.like('%within%'), ObsWithFlags.cell_method.like('%over%')))
    else:
        raw_filter = or_(ObsWithFlags.cell_method.like('%within%'), ObsWithFlags.cell_method.like('%over%'))

    # Get all of the variables for which observations exist
    # and iterate over them
    vars_ = sesh.query(ObsWithFlags.vars_id,ObsWithFlags.net_var_name)\
      .filter(ObsWithFlags.station_id == stn_id).filter(raw_filter)\
      .distinct().order_by(ObsWithFlags.vars_id)

    # Start with all of the times for which observations exist
    # and then use this as a basis for a left join
    # (sqlite doesn't support full outer joins
    times = sesh.query(ObsWithFlags.obs_time.label('obs_time')).filter(ObsWithFlags.station_id == stn_id).order_by(ObsWithFlags.obs_time).distinct()
    stmt = times.subquery()

    for vars_id, var_name in vars_.all():

        # Construct a query for all values of this variable
        right = sesh.query(ObsWithFlags.obs_time.label('obs_time'), ObsWithFlags.datum.label(var_name), ObsWithFlags.flag_name.label(var_name + '_flag'))\
          .filter(ObsWithFlags.vars_id == vars_id).filter(ObsWithFlags.station_id == stn_id).subquery()

        # Then join it to the query we're already building
        join_query = sesh.query(stmt, right).outerjoin(right, stmt.c.obs_time == right.c.obs_time)

        stmt = join_query.subquery()

    return sesh.query(stmt)
    #return join_query

def sql_station_table(sesh, stn_id):
    return compile_query(orm_station_table(sesh, stn_id))    
