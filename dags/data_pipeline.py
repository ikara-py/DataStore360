from datetime import datetime
import hashlib

import pandas as pd

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


RAW_PATH = "/opt/airflow/data/raw/store-data-6aa6d7a3f171f140353680(1).csv"
CLEANED_PATH = "/opt/airflow/data/processed/cleaned_data.csv"


def load_raw_csv():
    df = pd.read_csv(RAW_PATH)
    print(f"Loaded {len(df)} rows from raw CSV")


def clean_csv():
    df = pd.read_csv(RAW_PATH)

    df['State'] = df['State'].str.strip().str.title()
    df['City'] = df['City'].str.strip().str.title()
    df['Country'] = df['Country'].str.strip().str.title()
    df['Region'] = df['Region'].str.strip().str.title()
    df['Product Name'] = df['Product Name'].str.strip().str.title()
    df['Category'] = df['Category'].str.strip().str.title()

    df.drop_duplicates(inplace=True)

    mode_dict = (
        df.dropna(subset=['Postal Code'])
        .value_counts(['City', 'Postal Code'])
        .reset_index()
        .drop_duplicates('City')
        .set_index('City')['Postal Code']
        .to_dict()
    )

    get_null_idx = df[df['Postal Code'].isna()].index

    for idx in get_null_idx:
        city = df.at[idx, 'City']
        if city in mode_dict:
            df.at[idx, 'Postal Code'] = mode_dict[city]

    mode_dict = (
        df.dropna(subset=['Ship Mode'])
        .value_counts(['Customer ID', 'Ship Mode'])
        .reset_index()
        .drop_duplicates('Customer ID')
        .set_index('Customer ID')['Ship Mode']
        .to_dict()
    )

    get_null_idx = df[df['Ship Mode'].isna()].index

    for idx in get_null_idx:
        customer = df.at[idx, 'Customer ID']
        if customer in mode_dict:
            df.at[idx, 'Ship Mode'] = mode_dict[customer]

    mode_dict = (
        df.dropna(subset=['Customer Name'])
        .value_counts(['Customer ID', 'Customer Name'])
        .reset_index()
        .drop_duplicates('Customer ID')
        .set_index('Customer ID')['Customer Name']
        .to_dict()
    )

    df['Customer Name'] = df['Customer ID'].map(mode_dict)

    df['Order Date'] = pd.to_datetime(df['Order Date'], format='mixed')
    df['Ship Date'] = pd.to_datetime(df['Ship Date'], format='mixed')

    df.loc[df["Ship Mode"] == 'Same Day', 'Ship Date'] = df['Order Date']

    df["Ship Duration"] = df["Ship Date"] - df["Order Date"]

    mode_dict = (
        df.value_counts(['Ship Mode', 'Ship Duration'])
        .reset_index()
        .drop_duplicates('Ship Mode')
        .set_index('Ship Mode')['Ship Duration']
        .to_dict()
    )

    wrong_date_idx = df.loc[
        (df['Order Date'] > df['Ship Date']) | (df['Ship Date'].isna())
    ].index

    for i in wrong_date_idx:
        if df.at[i, 'Ship Mode'] in mode_dict:
            df.at[i, 'Ship Date'] = (
                df.at[i, 'Order Date']
                + mode_dict[df.at[i, 'Ship Mode']]
            )

    df["Ship Duration"] = df["Ship Date"] - df["Order Date"]

    idx_segment = df.loc[df['Segment'] == 'Consumerr'].index

    for i in idx_segment:
        df.at[i, 'Segment'] = 'Consumer'

    mode_dict = (
        df.value_counts(['Product Name', 'Product ID'])
        .reset_index()
        .drop_duplicates('Product Name')
        .set_index('Product Name')['Product ID']
        .to_dict()
    )

    for i in df.index:
        if df.at[i, 'Product Name'] in mode_dict:
            df.at[i, 'Product ID'] = mode_dict[df.at[i, 'Product Name']]

    conflicting_ids = (
        df.groupby('Product ID')['Product Name']
        .nunique()
    )

    conflicting_ids = conflicting_ids[conflicting_ids > 1].index

    for product_id in conflicting_ids:
        names = df.loc[
            df['Product ID'] == product_id,
            'Product Name'
        ].unique()

        for name in names[1:]:
            df.loc[
                (df['Product ID'] == product_id) &
                (df['Product Name'] == name),
                'Product ID'
            ] = product_id + '-DUP'

    df.loc[df['Customer Name'].isna(), 'Customer Name'] = 'Unknown'

    df['Customer Name'] = df['Customer Name'].apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()
    )

    df_clean = df[
        df['Sales'].notna() &
        df['Quantity'].notna() &
        (df['Quantity'] > 0) &
        (df['Discount'].between(0, 1))
    ].copy()

    df_clean['UnitPrice'] = (
        df_clean['Sales'] /
        (df_clean['Quantity'] * (1 - df_clean['Discount']))
    )

    mode_p = (
        df_clean.value_counts(['Product ID', 'UnitPrice'])
        .reset_index()
        .drop_duplicates('Product ID')
        .set_index('Product ID')['UnitPrice']
        .to_dict()
    )

    df['UnitPrice'] = df['Product ID'].map(mode_p)

    df.dropna(subset='UnitPrice', inplace=True)

    clone_df = df[
        df['Sales'].notna() &
        df['Quantity'].notna() &
        (df['Quantity'] > 0) &
        (df['Discount'].between(0, 1))
    ].copy()

    def calculate_solds(df, clone_df):
        clone_df['Sales'] = clone_df['Sales'].round(2)

        wrong_value = clone_df.loc[
            clone_df['Sales'] != (
                clone_df['UnitPrice'] *
                clone_df['Quantity'] *
                (1 - clone_df['Discount'])
            ).round(2)
        ].index

        for i in wrong_value:
            df.at[i, 'Sales'] = (
                df.at[i, 'UnitPrice'] *
                df.at[i, 'Quantity'] *
                (1 - df.at[i, 'Discount'])
            ).round(2)

    calculate_solds(df, clone_df)

    clone_2 = df[
        df['Sales'].notna() &
        df['Quantity'].notna() &
        (df['Quantity'] > 0)
    ].copy()

    wrong_discount = clone_2.loc[
        clone_2['Discount'].round(2) !=
        (
            1 -
            (
                clone_2['Sales'] /
                (clone_2['UnitPrice'] * clone_2['Quantity'])
            )
        ).round(2)
    ].index

    for i in wrong_discount:
        df.at[i, 'Discount'] = (
            1 -
            (
                df.at[i, 'Sales'] /
                (df.at[i, 'UnitPrice'] * df.at[i, 'Quantity'])
            )
        ).round(2)

    clone_df = df[
        df['Quantity'].notna() &
        (df['Quantity'] > 0)
    ].copy()

    calculate_solds(df, clone_df)

    clean_quantity = df[
        (df['Quantity'].notna()) &
        (df['Sales'].notna())
    ].copy()

    mode_quantity = (
        clean_quantity.value_counts(['Product ID', 'Quantity'])
        .reset_index()
        .drop_duplicates('Product ID')
        .set_index('Product ID')['Quantity']
        .to_dict()
    )

    empty_quantities = df.loc[
        (df['Quantity'].isna()) |
        (df['Quantity'] < 0)
    ].index

    for i in empty_quantities:
        df.at[i, 'Quantity'] = mode_quantity[df.at[i, 'Product ID']]

    wrong_value = df.loc[
        df['Sales'] != (
            df['UnitPrice'] *
            df['Quantity'] *
            (1 - df['Discount'])
        ).round(2)
    ].index

    for i in wrong_value:
        df.at[i, 'Sales'] = (
            df.at[i, 'UnitPrice'] *
            df.at[i, 'Quantity'] *
            (1 - df.at[i, 'Discount'])
        ).round(2)

    df.drop(columns=['Profit'], inplace=True)

    df['Quantity'] = df['Quantity'].astype(int)

    df['Segment'] = df['Segment'].replace({
        'Corporrate': 'Corporate',
        'Home Ofice': 'Home Office'
    })

    df.drop_duplicates(inplace=True)

    df['Row ID'] = range(1, len(df) + 1)

    df.to_csv(CLEANED_PATH, index=False)

    print(f"Cleaned CSV written to {CLEANED_PATH}")
    print(f"Final rows: {len(df)}")


def export_to_postgres():
    import pandas as pd
    from sqlalchemy import create_engine, text

    df = pd.read_csv(CLEANED_PATH)

    df["Order Date"] = pd.to_datetime(df["Order Date"]).dt.date
    df["Ship Date"] = pd.to_datetime(df["Ship Date"]).dt.date
    df["Ship Duration"] = pd.to_timedelta(df["Ship Duration"]).apply(
        lambda x: f"{x.days} days {x.seconds} seconds"
    )
    engine = create_engine(
        "postgresql+psycopg2://DataStore360:notroot@datastore-postgres:5432/DataStore360"
    )

    with engine.begin() as connection:
        with open("/opt/airflow/include/sql/schemas.sql") as file:
            connection.execute(text(file.read()))

        with open("/opt/airflow/include/sql/staging.sql") as file:
            connection.execute(text(file.read()))

        connection.execute(
            text("TRUNCATE TABLE staging.cleaned_data")
        )

        df.to_sql(
            "cleaned_data",
            connection,
            schema="staging",
            if_exists="append",
            index=False
        )

        connection.execute(
            text(
                "DROP TABLE IF EXISTS "
                "core.orders, core.products, core.customers "
                "CASCADE"
            )
        )

        with open("/opt/airflow/include/sql/core.sql") as file:
            connection.execute(text(file.read()))

        with open("/opt/airflow/include/sql/loading.sql") as file:
            connection.execute(text(file.read()))

        with open("/opt/airflow/include/sql/validation.sql") as file:
            validation_sql = file.read()

        for statement in validation_sql.split(";"):
            statement = statement.strip()

            if statement:
                result = connection.execute(text(statement))

                if result.returns_rows:
                    print(result.fetchall())

    print("Data exported successfully to PostgreSQL.")

with DAG(
    dag_id="data_store_360",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    load = PythonOperator(
        task_id="load_raw_csv",
        python_callable=load_raw_csv,
    )

    clean = PythonOperator(
        task_id="clean_csv",
        python_callable=clean_csv,
    )

    export = PythonOperator(
        task_id="export_to_postgres",
        python_callable=export_to_postgres,
    )

    load >> clean >> export