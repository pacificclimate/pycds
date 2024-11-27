## Hx table: Referential integrity on item delete

Referential integrity is enforced in the primary table, and therefore we do not have
to enforce it here. Specifically, we do not have to check manually whether deleting an
item is OK; referential integrity in the primary table ensures that it is, and our only
responsibility is to record that approved action in the hx table. Therefore the 
referential integrity maintenance triggers required for the view-based implementation 
are not necessary for this (table-based) implementation.