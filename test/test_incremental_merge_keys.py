def test_incremental_merge_deduplicates_races_by_raceid(spark):
    main_df = spark.createDataFrame([
        (1, "Race A", 2023),
        (2, "Race B", 2023)
    ], ["raceid", "name", "year"])

    inc_df = spark.createDataFrame([
        (2, "Race B", 2023),
        (3, "Race C", 2024)
    ], ["raceid", "name", "year"])

    merged = main_df.unionByName(inc_df).dropDuplicates(["raceid"])

    ids = sorted([r["raceid"] for r in merged.collect()])
    assert ids == [1, 2, 3]