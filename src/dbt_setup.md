# DBT installation guide

https://docs.getdbt.com/guides/manual-install?step=1

1. Install dbt

    ```bash
    pip install dbt dbt-postgres
    ```

2. Create a new dbt project

    ```bash
    dbt init my_new_project
    ```

3. Fill survey

   Enter a number: 1 (for postgres)
   host: localhost (actually, should be the host of your database)
   port: 5432
   user: postgres (username and password for the database)
   password: postgres
   dbname: air_quality (name of the database)
   schema: dbt
   threads: 1

4. Change directory to the new project

    ```bash
    cd my_new_project
    ```

5. Run dbt

    ```bash
    dbt run
    ```

   ```commandline
   dbt run
   23:29:31  Running with dbt=1.9.2
   23:29:31  Registered adapter: postgres=1.9.0
   23:29:31  Unable to do partial parsing because saved manifest not found. Starting full parse.
   23:29:32  Found 2 models, 4 data tests, 433 macros
   23:29:32  
   23:29:32  Concurrency: 1 threads (target='dev')
   23:29:32  
   23:29:32  1 of 2 START sql table model dbt.my_first_dbt_model ............................ [RUN]
   23:29:32  1 of 2 OK created sql table model dbt.my_first_dbt_model ....................... [SELECT 2 in 0.07s]
   23:29:32  2 of 2 START sql view model dbt.my_second_dbt_model ............................ [RUN]
   23:29:32  2 of 2 OK created sql view model dbt.my_second_dbt_model ....................... [CREATE VIEW in 0.03s]
   23:29:32  
   23:29:32  Finished running 1 table model, 1 view model in 0 hours 0 minutes and 0.37 seconds (0.37s).
   23:29:32  
   23:29:32  Completed successfully
   23:29:32  
   23:29:32  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
   ```

6. Profiles – https://docs.getdbt.com/docs/core/connect-data-platform/postgres-setup

    ```bash
    touch profiles.yml
    ```

    ```yaml
   config:
       send_anonymous_usage_stats: false
   dbt_weather_project:
     target: dev
     outputs:
       dev:
         type: &type postgres
         host: &host "{{ env_var('POSTGRES_HOST') }}"
         user: &user "{{ env_var('POSTGRES_USER') }}"
         password: &password "{{ env_var('POSTGRES_PASSWORD') }}"
         port: &port 5432
         dbname: air_quality
         schema: dbt
         threads: 1 
         connect_timeout: 10 # default 10 seconds
   
    ```

7. Run dbt from console

    ```bash
    export $(xargs < .env)
    dbt run 
    ```

8. Add elementary - https://docs.elementary-data.com/cloud/onboarding/quickstart-dbt-package
    - Add profiles.yml with elementary setup
    - run `dbt deps` to install elementary
    - run `dbt test` to test with elementary

9. Add elementary package with pip install

    ```bash
    pip install elementary-data
    ```
   If you have a problem with the installation, try to upgrade pip and setuptools.
   `pip install --upgrade setuptools pip`


10. Add edr report - https://docs.elementary-data.com/oss/guides/generate-report-ui
    
