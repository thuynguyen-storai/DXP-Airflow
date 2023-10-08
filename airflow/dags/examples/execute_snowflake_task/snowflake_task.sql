BEGIN
    CREATE TABLE IF NOT EXISTS public.test_airflow (
        id INT,
        "name" VARCHAR,
        inserted_date TIME
    );

    LET random_id := random();
    LET random_name := randstr(5, random());

    INSERT INTO public.test_airflow VALUES (
        :random_id, :random_name, CURRENT_TIME()
    );
END;
