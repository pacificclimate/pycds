from pycds import Contact, Network

def test_have_contacts(test_session):
    q = test_session.query(Contact.name)
    rv = set([row.name for row in q])
    assert rv == set(['Eric Meyer', 'Simon Walker'])

def test_contacts_relation(test_session):
    q = test_session.query(Contact).join(Network).filter(Network.name == 'MoTIe')
    contact = q.first()
    assert contact.name == 'Simon Walker'
