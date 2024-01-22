# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/pos-dlt. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/real-time-point-of-sale-analytics.

# COMMAND ----------

# MAGIC %md This notebook was developed to run as part of a DLT pipeline. Details on the scheduling of the DLT jobs are provided in the *POS 05* notebook.

# COMMAND ----------

# MAGIC %md **IMPORTANT NOTE** Do not attempt to interactively run the code in this notebook.  **This notebook must be scheduled as a DLT pipeline.**  If you attempt to run the notebook interactively, *e.g.* by running individual cells or clicking *Run All* from the top of the notebook you will encounter errors.  Job scheduling is addressed in the *POS 05* notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC The purpose of this notebook is to calculate the current-state inventory of products in various store locations leveraging data arriving in near real-time from point-of-sale systems.  Those data are exposed as Delta Live Table (DLT) objects in the *POS 3* notebook.
# MAGIC
# MAGIC This notebook should be scheduled to run while the *POS 02* notebook (which generates the simulated event data) runs on a separate cluster.  It also depends on the demo environment having been configured per the instructions in the *POS 01* notebook.


# COMMAND ----------
dlt.table(
    name="inventory_current_python",
    comment="current inventory count for a product in a store location",
    table_properties={"quality": "gold"},
    spark_conf={"pipelines.trigger.interval": "5 minutes"},
)


def inventory_current_python():
    # calculate inventory change with bopis corrections
    inventory_change_df = (
        dlt.read("inventory_change")
        .alias("x")
        .join(dlt.read("store").alias("y"), on="store_id")
        .join(dlt.read("inventory_change_type").alias("z"), on="change_type_id")
        .filter(f.expr("NOT(y.name='online' AND z.change_type='bopis')"))
        .select("store_id", "item_id", "date_time", "quantity")
    )

    # calculate current inventory
    inventory_current_df = (
        dlt.read("latest_inventory_snapshot")
        .alias("a")
        .join(
            inventory_change_df.alias("b"),
            on=f.expr(
                """
            a.store_id=b.store_id AND
            a.item_id=b.item_id AND
            a.date_time<=b.date_time
            """
            ),
            how="leftouter",
        )
        .groupBy("a.store_id", "a.item_id")
        .agg(
            first("a.quantity").alias("snapshot_quantity"),
            sum("b.quantity").alias("change_quantity"),
            first("a.date_time").alias("snapshot_datetime"),
            max("b.date_time").alias("change_datetime"),
        )
        .withColumn("change_quantity", f.coalesce("change_quantity", f.lit(0)))
        .withColumn("current_quantity", f.expr("snapshot_quantity + change_quantity"))
        .withColumn("date_time", f.expr("GREATEST(snapshot_datetime, change_datetime)"))
        .drop("snapshot_datetime", "change_datetime")
        .orderBy("current_quantity")
    )

    return inventory_current_df


# COMMAND ----------

# MAGIC %md
# MAGIC It's important to note that the current inventory table is implemented using a 5-minute recalculation.
# MAGIC - While DLT supports near real-time streaming, the business objectives associated with the calculation of near current state inventories do not require up to the second precision.
# MAGIC - Instead, 5-, 10- and 15-minute latencies are often preferred to give the data some stability and to reduce the computational requirements associated with keeping current.
# MAGIC - From a business perspective, responses to diminished inventories are often triggered when values fall below a threshold that's well-above the point of full depletion (as lead times for restocking may be measured in hours, days or even weeks).
# MAGIC - With that in mind, the 5-minute interval used here exceeds the requirements of many retailers.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | azure-iot-device                                     | Microsoft Azure IoT Device Library | MIT    | https://pypi.org/project/azure-iot-device/                       |
# MAGIC | azure-storage-blob                                | Microsoft Azure Blob Storage Client Library for Python| MIT        | https://pypi.org/project/azure-storage-blob/      |
