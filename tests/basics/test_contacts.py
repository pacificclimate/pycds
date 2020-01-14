from pycds import Contact, Network


def test_have_contacts(tfs_pycds_sesh_with_small_data):
    q = tfs_pycds_sesh_with_small_data.query(Contact.name)
    rv = set([row.name for row in q])
    assert rv == set(['Eric', 'Simon', 'Pat'])


def test_contacts_relation(tfs_pycds_sesh_with_small_data):
    q = tfs_pycds_sesh_with_small_data.query(Contact).join(Network).filter(Network.name == 'MoTIe')
    contact = q.first()
    assert contact.name == 'Simon'
