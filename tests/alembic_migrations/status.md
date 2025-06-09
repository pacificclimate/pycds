~~a59d64cf16ca -> 8c05da87cb79 (head), Add history tracking to obs_raw~~
~~7ab87f8fbcf4 -> a59d64cf16ca, Add history tracking to 4 main metadata tables~~
~~081f17262852 -> 7ab87f8fbcf4, Add change history utilities~~
~~3505750d3416 -> 081f17262852, convert monthly_total_precipitation_mv to native matview~~
~~efde19ea4f52 -> 3505750d3416, Add start/stop times to vars_per_history_mv~~
~~6cb393f711c3 -> efde19ea4f52, Fix function getstationvariabletable~~
fecff1a73d7e -> 6cb393f711c3, Add columns vars_ids and unique_variable_tags to crmp_network_geoserver
bb2a222a1d4a -> fecff1a73d7e, Add columns vars_ids and unique_variable_tags to matview collapsed_vars_mv
~~22819129a609 -> bb2a222a1d4a, Convert obs_count_per_month_history_mv to native matview~~
~~bf366199f463 -> 22819129a609, Convert collapsed_vars_mv to native matview~~
~~96729d6db8b3 -> bf366199f463, Convert station_obs_stats_mv to native matview~~
~~5c841d2c01d1 -> 96729d6db8b3, Convert climo_obs_count_mv to native matview~~
~~78260d36e42b -> 5c841d2c01d1, Add whitespace constraints to meta_vars columns~~
~~83896ee79b06 -> 78260d36e42b, Add meta_vars.net_var_name valid identifier constraint~~
~~879f0efa125f -> 83896ee79b06, Add function variable_tags~~
~~7b139906ac46 -> 879f0efa125f, add publish column to meta station~~
~~3d50ec832e47 -> 7b139906ac46, add not null constraint on meta vars~~
0d99ba90c229 -> 3d50ec832e47, add constraint on meta vars
~~2914c6c8a7f9 -> 0d99ba90c229, Create missing indexes~~
~~e688e520d265 -> 2914c6c8a7f9, Drop meta_climo_attrs~~
~~bdc28573df56 -> e688e520d265, Refactor flags association tables~~
~~7a3b247c577b -> bdc28573df56, Add obs_raw indexes~~
~~8fd8f556c548 -> 7a3b247c577b, Add VarsPerHistory native matview~~
~~84b7fc2596d5 -> 8fd8f556c548, Add weather anomaly matviews~~
~~4a2f1879293a -> 84b7fc2596d5, Add utility views~~
~~522eed334c85 -> 4a2f1879293a, Create functions~~
<base> -> 522eed334c85, Create initial database