# Flight templates

Flight templates are small Python programs with a `main()` entrypoint and a
`requirements.txt` file. They are intended to be selected from the Flight UI or
referenced by cookbook recipes.

Available templates:

- [dbt-runner](dbt-runner): clone a dbt project, write a runtime profile, run
  dbt, and record an audit row in MotherDuck.
