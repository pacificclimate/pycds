from pycds import Network

def create_PCIC_derived_variable_network(session):
    """Create the synthetic network for derived variables"""
    name = 'PCIC Derived Variables'
    if session.query(Network).filter(Network.name == name).count() == 0:
        session.add(
            Network(
                name=name,
                long_name='Synthetic network for derived variables computed by PCIC',
                # virtual='???',   # TODO: What does this mean? No existing networks define it
                publish = False, # TODO: What does this mean?
                # color = '#??????', # TODO: Does this need to be defined?
            )
        )
    session.flush()
