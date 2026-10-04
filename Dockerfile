# Astro Runtime 3.3 = Airflow 3.3. If `astro dev init` gives you a newer FROM line, use it.
FROM astrocrpublic.azurecr.io/astronomer/astro-runtime:3.3-2

# dbt lives in its own virtualenv so its dependencies never fight Airflow's.
RUN python -m venv dbt_venv \
 && dbt_venv/bin/pip install --no-cache-dir "dbt-snowflake~=1.12.0" \
 && cd include/dbt && ../../dbt_venv/bin/dbt deps

# Pre-fetch hadoop-aws + AWS SDK jars into the image so Spark tasks don't download
# them at runtime (large, one-time). Java comes from packages.txt.
RUN python -c "import sys; sys.path.insert(0, 'include/spark'); \
from common import build_spark; build_spark('warm_jars', uses_s3=True).stop()"
