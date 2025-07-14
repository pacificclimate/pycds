WITH network_ids AS
    (SELECT DISTINCT crmp.meta_network.network_id AS network_id
     FROM crmp.meta_network JOIN crmp.meta_station ON crmp.meta_station.network_id = crmp.meta_network.network_id
              JOIN crmp.meta_history ON crmp.meta_history.station_id = crmp.meta_station.station_id
     WHERE crmp.meta_history.province IN ('BC') AND crmp.meta_network.publish = true)
SELECT crmp.meta_vars.vars_id AS crmp_meta_vars_vars_id, crmp.meta_vars.net_var_name AS crmp_meta_vars_net_var_name, crmp.meta_vars.long_description AS crmp_meta_vars_long_description, crmp.meta_vars.unit AS crmp_meta_vars_unit, crmp.meta_vars.standard_name AS crmp_meta_vars_standard_name, crmp.meta_vars.cell_method AS crmp_meta_vars_cell_method, crmp.meta_vars.precision AS crmp_meta_vars_precision, crmp.meta_vars.display_name AS crmp_meta_vars_display_name, crmp.meta_vars.short_name AS crmp_meta_vars_short_name, crmp.meta_vars.network_id AS crmp_meta_vars_network_id, variable_tags(meta_vars) AS tags
FROM crmp.meta_vars JOIN network_ids ON crmp.meta_vars.network_id = network_ids.network_id
ORDER BY crmp.meta_vars.vars_id ASC;