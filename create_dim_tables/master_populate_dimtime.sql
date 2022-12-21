insert into master.DimTime 
  select * from staging.dim_time;