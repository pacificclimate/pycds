from pycds import Contact, Network


def test_have_contacts(pycds_sesh_with_small_data):
    q = pycds_sesh_with_small_data.query(Contact.name)
    rv = set([row.name for row in q])
    assert rv == set(['Eric', 'Simon', 'Pat'])


def test_contacts_relation(pycds_sesh_with_small_data):
    q = pycds_sesh_with_small_data.query(Contact).join(Network).filter(Network.name == 'MoTIe')
    contact = q.first()
    assert contact.name == 'Simon'
