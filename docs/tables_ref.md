# Queries for Any Group

## Initializing the Query Object

  ```py
  q = Queries(t1 = '2023-10-01', t2 = '2023-12-31', print_SQL = True, 
  clipboard = True)
  ```
## Demographics
::: script.nice_sql.Queries
    options:
      heading_level: 3
      show_root_heading: False
      show_root_toc_entry: false
      show_root_full_path: false
      show_root_members_full_path: false
      filters: ["^dem"]
      separate_signature: true
      docstring_options:
        ignore_init_summary: false
      merge_init_into_class: false


## Legal
::: script.nice_sql.Queries
    options:
      heading_level: 3
      show_root_heading: False
      show_root_toc_entry: false
      show_root_members_full_path: false
      filters: ["^legal"]
      docstring_options:
        ignore_init_summary: true

## Linkages
::: script.nice_sql.Queries
    options:
      heading_level: 3
      show_root_heading: False
      show_root_toc_entry: false
      show_root_members_full_path: false
      filters: ["^link"]

## Programs
::: script.nice_sql.Queries
    options:
      heading_level: 3
      show_root_heading: False
      show_root_toc_entry: false
      show_root_members_full_path: false
      filters: ["^program"]
