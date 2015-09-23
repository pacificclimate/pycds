from pycds import Contact, Network

def test_have_contacts(test_session):
    q = test_session.query(Contact.name)
    rv = set([row.name for row in q])
    assert rv == set(['Eric', 'Simon', 'Pat'])

def test_contacts_relation(test_session):
    q = test_session.query(Contact).join(Network).filter(Network.name == 'MoTIe')
    contact = q.first()
    assert contact.name == 'Simon'
